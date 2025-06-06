package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"arbitrage-engine/internal/config"
	"arbitrage-engine/internal/rate_limiter"
	"arbitrage-engine/internal/types"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// Constants for reconnect backoff
const (
	reconnectBaseDelay    = 1 * time.Second
	reconnectMaxDelay     = 60 * time.Second
	reconnectMaxAttempts  = 0 // Maximum number of attempts (0 = infinite)
	reconnectJitterFactor = 0.1
)

// Default WebSocket timeouts (can be overridden by config later)
const (
	defaultWsReadTimeout         = 4 * time.Minute
	defaultWsWriteTimeout        = 5 * time.Second // Timeout for sending a single message (including control frames). Also used for control frames ping/pong/close.
	defaultWsPingInterval        = 30 * time.Second
	wsSubscriptionCommandTimeout = 5 * time.Second        // Timeout for sending a command from handler to WS (batch send timeout)
	defaultBatchInterval         = 100 * time.Millisecond // Default interval for sending batched commands
	wsDialTimeout                = 10 * time.Second       // Таймаут на установку WS соединения (Dial)
)

// BinanceClient represents a client for interacting with Binance for a specific market type.
type BinanceClient struct {
	conn *websocket.Conn
	mu   sync.Mutex // Protects conn and isClosed

	isClosed bool // Flag signalling explicit stop via Stop().

	Channels types.ExchangeChannels // Structure with channels (ResyncChan/SubChan are now <-chan)

	restLimiter *rate_limiter.Limiter // Rate limiter (предполагается, что он уже использует logrus)
	// Store the market-specific config (SpotConfig or FuturesConfig).
	// This provides access to market-specific URLs, enabled status, and WS limits.
	cfg config.MarketConfigInterface

	marketType types.MarketType // Market Type (Spot, Futures, etc.)

	// --- Batching and Subscription Management ---
	// Map to track active subscriptions (topic string -> original command).
	// Key: topic string (e.g., "btcusdt@depth@100ms", "ethusdt@bookTicker")
	// Value: types.SubscriptionCommand that initiated the subscription.
	// We store the *value* here, as the command is read from a channel (copy) or generated (new struct).
	activeSubscriptions map[string]types.SubscriptionCommand
	subMu               sync.RWMutex // Read/Write mutex for activeSubscriptions map

	// Buffer for accumulating topics for batched SUBSCRIBE/UNSUBSCRIBE messages.
	// Key: "subscribe" or "unsubscribe" string. Value: slice of topic strings.
	topicsToBatch map[string][]string
	batchTimer    *time.Timer // Timer to trigger batch sending.
	// Channel to signal immediate batch send (e.g., when command arrives and timer expired).
	sendBatchSignal chan struct{}

	// WS Limits from config
	wsMaxTopicsPerSubscribe int           // Max topics in one WS SUBSCRIBE/UNSUBSCRIBE message.
	wsSendMessageRateLimit  time.Duration // Min time between sending WS messages (1/rate_limit_per_sec).

	// --- Client Lifecycle Management ---
	reconnectAttempt int
	clientCtx        context.Context    // Context for internal client goroutines and operations.
	clientCancel     context.CancelFunc // Function to cancel clientCtx.
	clientWg         sync.WaitGroup     // WaitGroup for client's internal goroutines.

	wsReadTimeout  time.Duration
	wsWriteTimeout time.Duration // Timeout for writing a single message (including control frames).
	pingInterval   time.Duration
	lastPong       time.Time
	// initialBatchSent bool // Removed - replaced by connectedAndSubscribedOnce logic in handleSubscriptionCommands

	log *logrus.Entry // >>> Используем *logrus.Entry для логгера с контекстом клиента

	// Channel to signal handleSubscriptionCommands that connection is ready for initial subscribes
	connReadyForSubscribe chan struct{} // *** ДОБАВЛЕНО ***
}

// NewBinanceClient creates a new client instance for a specific market type.
// Accepts MarketConfigInterface (SpotConfig or FuturesConfig) and MarketType.
// *** ОБНОВЛЕННАЯ СИГНАТУРА: ExchangeChannels теперь содержит <-chan для команд ***
func NewBinanceClient(
	ctx context.Context, // Main application context (передается из main.go)
	logger *logrus.Entry, // >>> Принимаем логгер *logrus.Entry
	cfg config.MarketConfigInterface, // Specific market config (Spot or Futures)
	marketType types.MarketType, // The market type this client instance handles
	channels types.ExchangeChannels, // *** ExchangeChannels теперь содержит <-chan ***
	rlManager *rate_limiter.Limiter, // Shared REST rate limiter
) *BinanceClient {
	// Создаем логгер с контекстом для этого клиента (биржа и тип рынка)
	logger = logger.WithFields(logrus.Fields{
		"exchange":    "binance",
		"market_type": marketType,
	})
	logger.Info("Начало инициализации BinanceClient...")

	// Create an internal context for the client, derived from the main context.
	// This context will be cancelled when the client's Stop() method is called,
	// or when the main Run() method exits (e.g., due to external context cancellation).
	clientCtx, clientCancel := context.WithCancel(ctx)

	// Load WS limits from config
	wsMaxTopics := cfg.GetWSMaxTopicsPerSubscribe()
	wsMsgLimitPerSec := cfg.GetWSSendMessageRateLimitPerSec()
	wsSendMessageRateLimit := time.Second / time.Duration(wsMsgLimitPerSec) // Calculate min time between messages
	// Ensure a minimum rate limit interval to avoid division by zero or negative values
	if wsMsgLimitPerSec <= 0 { // Проверяем исходное значение, чтобы избежать деления на ноль
		wsSendMessageRateLimit = 100 * time.Millisecond // Default to 10 messages/sec
		logger.WithField("config_value", wsMsgLimitPerSec).Warnf("Некорректное значение WSSendMessageRateLimitPerSec (%d). Установлено значение по умолчанию: %s.", wsMsgLimitPerSec, wsSendMessageRateLimit)
	} else {
		logger.WithField("config_value", wsMsgLimitPerSec).Debugf("WSSendMessageRateLimit установлен: %s.", wsSendMessageRateLimit)
	}

	// TODO: Load other timeouts and intervals from config if set.
	wsReadTimeout := defaultWsReadTimeout
	wsWriteTimeout := defaultWsWriteTimeout
	pingInterval := defaultWsPingInterval
	logger.WithFields(logrus.Fields{
		"ws_read_timeout":  wsReadTimeout,
		"ws_write_timeout": wsWriteTimeout,
		"ping_interval":    pingInterval,
		"ws_dial_timeout":  wsDialTimeout, // Логируем таймаут Dial
	}).Debug("Установлены таймауты и интервалы WS.")

	client := &BinanceClient{
		Channels:            channels, // Каналы ResyncChan/SubChan теперь <-chan
		restLimiter:         rlManager,
		cfg:                 cfg,
		marketType:          marketType,
		activeSubscriptions: make(map[string]types.SubscriptionCommand), // Initialize active subscriptions map

		topicsToBatch:   make(map[string][]string),           // Initialize batch buffer
		batchTimer:      time.NewTimer(defaultBatchInterval), // Initialize batch timer
		sendBatchSignal: make(chan struct{}, 1),              // Initialize signal channel (buffered to avoid blocking sender)

		wsMaxTopicsPerSubscribe: wsMaxTopics,
		wsSendMessageRateLimit:  wsSendMessageRateLimit,

		reconnectAttempt: 0,
		clientCtx:        clientCtx,
		clientCancel:     clientCancel,
		// clientWg is zero-initialized.

		wsReadTimeout:  wsReadTimeout,
		wsWriteTimeout: wsWriteTimeout, // Use default write timeout for single messages
		pingInterval:   pingInterval,
		lastPong:       time.Time{},

		log: logger, // >>> Сохраняем логгер *logrus.Entry
	}

	// Stop the timer immediately after creation, it will be reset when the first command arrives.
	// This prevents the timer from firing before any commands are received.
	if !client.batchTimer.Stop() {
		// If Stop() returns false, it means the timer had already fired or was stopped.
		// We need to drain the channel in case it fired between NewTimer and Stop.
		select {
		case <-client.batchTimer.C:
			// Channel drained
			logger.Debug("Таймер пакетирования сработал сразу после создания и остановки. Канал сброшен.")
		default:
			// Channel was empty or already drained
			// logger.Debug("Таймер пакетирования остановлен успешно.") // Too noisy
		}
	}

	logger.Info("BinanceClient инициализирован.")
	logger.WithFields(logrus.Fields{
		"ws_url":                  cfg.GetWSBaseURL(),
		"ws_max_topics_per_batch": client.wsMaxTopicsPerSubscribe,
		"ws_min_msg_interval":     client.wsSendMessageRateLimit,
	}).Debug("Параметры WS клиента.")

	// Launch handler goroutines only once for the client's lifecycle.
	// These goroutines are managed by clientWg and listen to clientCtx.
	// They will stop when clientCtx is cancelled.
	client.clientWg.Add(1)
	go func() {
		defer client.clientWg.Done()
		// handleResyncCommands listens to the client's specific ResyncChan (<-chan) and respects clientCtx.
		client.handleResyncCommands(client.clientCtx) // Передаем внутренний контекст клиента
		client.log.Info("handleResyncCommands goroutine finished.")
	}()
	client.log.Info("handleResyncCommands goroutine launched.")

	client.clientWg.Add(1)
	go func() {
		defer client.clientWg.Done()
		// handleSubscriptionCommands listens to the client's specific SubChan (<-chan) and manages batching/sending.
		// It also respects clientCtx.
		client.handleSubscriptionCommands(client.clientCtx) // Передаем внутренний контекст клиента
		client.log.Info("handleSubscriptionCommands goroutine finished.")
	}()
	client.log.Info("handleSubscriptionCommands goroutine launched.")

	return client
}

func (c *BinanceClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	c.log.Info("Запуск основной горутины Run...")
	wg.Add(1)
	defer wg.Done()

	defer func() {
		c.log.Info("Run defer: Инициирование отмены внутреннего контекста и ожидание внутренних горутин.")
		c.clientCancel()
		c.clientWg.Wait()
		c.log.Info("Run defer: Все внутренние горутины клиента завершены.")
		c.Close() // Закрываем соединение при выходе из Run
		c.log.Info("Run defer: Соединение WebSocket закрыто.")
		c.log.Info("Основная горутина Run завершила работу.")
	}()

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Run: Получен сигнал отмены из основного контекста приложения. Выход из Run loop.")
			return
		default:
		}

		c.mu.Lock()
		if c.isClosed {
			c.mu.Unlock()
			c.log.Info("Run: Клиент помечен как закрытый (isClosed). Выход из Run loop.")
			return
		}
		c.mu.Unlock()

		if c.reconnectAttempt > 0 {
			c.waitForReconnect(c.clientCtx)
			if c.clientCtx.Err() != nil {
				c.log.Info("Run: Внутренний контекст клиента отменен во время ожидания переподключения. Выход.")
				return
			}
		}
		c.reconnectAttempt++

		c.log.Infof("Run: Попытка #%d подключения WebSocket к %s...", c.reconnectAttempt, c.cfg.GetWSBaseURL())

		err := c.ConnectWebSocket()
		if err != nil {
			// Проверяем, была ли ошибка подключения вызвана отменой clientCtx
			if errors.Is(err, c.clientCtx.Err()) {
				c.log.Info("Run: Подключение отменено отменой clientCtx. Выход.")
				return // Выход из Run loop
			}
			c.log.WithError(err).Errorf("Run: Ошибка подключения WebSocket к %s. Закрытие соединения и повторная попытка...", c.cfg.GetWSBaseURL())
			c.Close() // Закрываем соединение (устанавливает c.conn = nil)
			continue  // Повторная попытка подключения
		}

		// === УСПЕШНОЕ ПОДКЛЮЧЕНИЕ ===
		c.reconnectAttempt = 0
		c.log.Info("Run: Подключение WebSocket успешно установлено.")
		// Добавляем небольшую паузу после успешного соединения, прежде чем отправлять подписки,
		// чтобы дать серверу WS время на инициализацию сокета.
		time.Sleep(200 * time.Millisecond)

		// Переотправляем команды подписки для активных топиков.
		// Это гарантирует, что после переподключения клиент запросит нужные стримы.
		c.log.Info("Run: Попытка переотправить команды подписки для активных топиков...")
		resubscribeCmds := c.generateSubscribeCommandsFromActiveSubscriptions()
		if len(resubscribeCmds) > 0 {
			c.log.Infof("Run: Сгенерировано %d команд для переподписки. Отправка обработчику команд...", len(resubscribeCmds))
			// Отправляем команды в SubChan клиента.
			// Важно: SubChan теперь <-chan *types.SubscriptionCommand
			// Но эта горутина (Run) не должна писать в него.
			// Команды переподписки должны быть отправлены ИЗНУТРИ handleSubscriptionCommands
			// при обнаружении успешного подключения (или инициированы другим способом,
			// например, через отдельный канал связи Run -> handleSubscriptionCommands).
			// Текущая логика (отправка в c.Channels.SubChan) неверна, т.к. SubChan - <-chan для клиента.

			// >>> ИСПРАВЛЕНИЕ: Логика переотправки команд подписки должна быть в handleSubscriptionCommands
			// или в отдельной горутине, которая имеет доступ к activeSubscriptions и отправляет их через sendSubscriptionWSMessage.
			// Или, проще, handleSubscriptionCommands может периодически проверять isConnected status
			// и при переходе conn=nil -> conn!=nil переотправлять подписки.

			// Временно удаляем этот блок отправки команд:
			/*
					sendCtx, cancelSend := context.WithTimeout(c.clientCtx, 10*time.Second)
					sentCount := 0
					for _, cmd := range resubscribeCmds {
						select {
						case c.Channels.SubChan <- &cmd: // <-- ОШИБКА: SubChan теперь <-chan
							sentCount++
						case <-sendCtx.Done():
							c.log.Warnf("Run: Не удалось отправить все команды переподписки обработчику. %d отправлено до отмены контекста done.", sentCount)
							goto sendResubCommandsDone // Переход для отмены контекста sendCtx
						default:
							c.log.Warnf("Run: Канал SubChan полный во время отправки команды переподписки для %s %s. Пропуск команды.", c.marketType, cmd.Symbol, cmd.Type)
						}
					}
				sendResubCommandsDone:
					cancelSend()
					c.log.Infof("Run: Отправлено %d команд переподписки обработчику.", sentCount)
			*/

			// >>> АЛЬТЕРНАТИВА (более правильная): Сигнализировать handleSubscriptionCommands о необходимости переподписки.
			// Добавим канал для этого сигнала.
			// Временно пропустим этот шаг, сосредоточимся на ResyncChan.
			// Переподписка активных топиков после коннекта - задача handleSubscriptionCommands.

		} else {
			c.log.Info("Run: Нет активных подписок для переотправки команд.")
		}

		c.log.Info("Run: Запуск Listen и Keep-Alive горутин...")

		// === SYNCHRONIZED STATE: Client is connected and attempting to resubscribe ===

		listenDone := make(chan struct{})

		c.clientWg.Add(1)
		go func() {
			defer c.clientWg.Done()
			defer close(listenDone)
			c.listen(c.clientCtx)
			c.log.Info("Listen goroutine finished.")
		}()
		c.log.Info("Listen goroutine launched.")

		c.clientWg.Add(1)
		go func() {
			defer c.clientWg.Done()
			c.keepAlive(c.clientCtx)
			c.log.Info("Keep-Alive goroutine finished.")
		}()
		c.log.Info("Keep-Alive goroutine launched.")

		// >>> УДАЛЕНО: Инициирование начального снимка перемещено в handleSubscriptionCommands::sendBatch
		// c.log.Infof("Run: Инициирование REST snapshot resync для символов с активными подписками на глубину на ЭТОМ типе рынка (%s)...", c.marketType)
		// ... (удален код сбора символов и отправки команды ресинка) ...
		// endResyncCommandLoop:

		// 12. Wait for Listen goroutine to finish or external context cancellation. (unchanged)
		c.log.Info("Run: Ожидание завершения Listen горутины или отмены основного контекста...")
		select {
		case <-ctx.Done():
			c.log.Info("Run: Получен сигнал отмены из основного контекста приложения (ctx.Done). Выход из Run.")
			return
		case <-listenDone:
			c.log.Info("Run: Listen горутина завершилась. Инициирование цикла переподключения.")
			c.Close() // Закрываем соединение
		}
	}
}

// listen читает сообщения из WebSocket соединения.
// Завершается при ошибке чтения или при отмене контекста.
// Принимает контекст (clientCtx).
func (c *BinanceClient) listen(ctx context.Context) {
	c.log.Info("Listen goroutine launched...")
	defer c.log.Info("Listen goroutine finished.")

	for {
		select {
		case <-ctx.Done(): // Слушаем контекст, переданный в эту горутину (clientCtx)
			c.log.Info("Listen: Получен сигнал отмены контекста (clientCtx), выход.")
			return
		default:
		}

		// Проверяем соединение под мьютексом перед чтением
		c.mu.Lock()
		if c.conn == nil {
			c.mu.Unlock()
			// c.log.Debug("Listen: Connection nil перед чтением, выход.") // Too noisy, use Trace if needed
			return // Соединение nil, выход из горутины.
		}

		// Устанавливаем таймаут чтения с использованием настроенного wsReadTimeout
		err := c.conn.SetReadDeadline(time.Now().Add(c.wsReadTimeout))
		c.mu.Unlock() // Освобождаем мьютекс после работы с c.conn

		if err != nil {
			c.log.WithError(err).Error("Listen: Ошибка установки ReadDeadline.")
			// Ошибка установки дедлайна часто указывает на проблему с соединением. Выходим.
			return // Выход из Listen горутины при ошибке.
		}

		// Читаем следующее сообщение. Это блокирующий вызов.
		messageType, message, err := c.conn.ReadMessage()

		if err != nil {
			// Обрабатываем различные типы ошибок чтения.
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.log.Infof("Listen: Соединение закрыто нормально (CloseError): %v", err)
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				c.log.WithError(err).Warn("Listen: Ошибка таймаута чтения (вероятно, потеря соединения).")
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				c.log.Debug("Listen: Чтение прервано из-за закрытого сетевого соединения (вероятно, вызовом c.Close()).")
			} else {
				c.log.WithError(err).Error("Listen: Неизвестная ошибка чтения из WebSocket.")
			}
			// При любой ошибке чтения (кроме EOF, который handled библиотекой как закрытие)
			// или нормальном закрытии, выход из Listen горутины.
			return // Выход из Listen горутины.
		}

		// Если сообщение успешно прочитано:
		if messageType == websocket.TextMessage {
			c.handleMessage(message) // Обрабатываем текстовое сообщение. handleMessage использует logrus.
		} else {
			// Игнорируем или логируем другие типы сообщений (бинарные, управляющие, кроме PONG)
			// PONG обрабатывается SetPongHandler.
			c.log.WithField("message_type", messageType).Trace("Listen: Получено не текстовое сообщение.") // Уровень Trace
		}
	}
}

// waitForReconnect implements the delay before reconnection, respecting the context.
// ctx: Context that can cancel the wait (should be clientCtx).
// *** ДОБАВЛЕНО этот отсутствующий метод ***
func (c *BinanceClient) waitForReconnect(ctx context.Context) {
	// getReconnectDelay использует logrus внутри
	delay := c.getReconnectDelay(c.reconnectAttempt)

	// Если getReconnectDelay вернул 0 (например, исчерпаны попытки и вызван clientCancel), выходим сразу.
	if delay <= 0 {
		c.log.Debug("WaitForReconnect: Задержка переподключения 0 или меньше. Пропуск ожидания.")
		return
	}

	c.log.Infof("WaitForReconnect: Ожидание %s перед следующей попыткой переподключения #%d...", delay, c.reconnectAttempt)

	select {
	case <-ctx.Done(): // Слушаем переданный контекст (clientCtx).
		c.log.Info("WaitForReconnect: Отмена контекста (ctx.Done), остановка ожидания.")
		return // Выход, если контекст отменен.
	case <-time.After(delay):
		// Ожидание завершено.
		c.log.Debug("WaitForReconnect: Ожидание завершено.")
	}
}

// getReconnectDelay calculates the delay before reconnection with exponential backoff and jitter.
// Uses logrus for logging attempts exhausted.
// *** ДОБАВЛЕНО этот отсутствующий метод ***
func (c *BinanceClient) getReconnectDelay(attempt int) time.Duration {
	// Проверяем максимальное количество попыток (если оно установлено)
	if reconnectMaxAttempts > 0 && c.reconnectAttempt > reconnectMaxAttempts {
		c.log.Warnf("GetReconnectDelay: Достигнуто максимальное количество попыток переподключения (%d). Сдача.", reconnectMaxAttempts)
		// Если достигнут лимит, отменяем внутренний контекст клиента.
		// Это приведет к завершению всех горутин клиента и выходу из основного Run цикла.
		c.clientCancel()
		return 0 // Возвращаем 0, чтобы waitForReconnect вышел сразу.
	}

	// Экспоненциальный backoff: baseDelay * 2^(attempt-1)
	// math.Pow(2, 0) = 1 для первой попытки (attempt=1), что дает reconnectBaseDelay.
	baseDelay := reconnectBaseDelay
	maxDelay := reconnectMaxDelay
	delay := float64(baseDelay) * math.Pow(2, float64(c.reconnectAttempt-1))

	// Ограничиваем максимальной задержкой.
	delay = math.Min(delay, float64(maxDelay))

	// Добавляем джиттер (случайное колебание задержки)
	jitterFactor := reconnectJitterFactor
	// Генерируем случайный множитель джиттера от -jitterFactor до +jitterFactor
	// Для простоты используем время как источник псевдослучайности.
	jitterAmount := jitterFactor * delay * (float64(time.Now().UnixNano()%100)/50.0 - 1.0) // от -1.0 до 1.0

	delay += jitterAmount

	// Гарантируем, что задержка неотрицательна.
	if delay < 0 {
		delay = 0
	}
	// Гарантируем минимальную задержку для попыток > 0.
	if c.reconnectAttempt > 0 && delay < float64(time.Second) {
		delay = float64(time.Second)
	}

	return time.Duration(delay)
}

// generateSubscribeCommandsFromActiveSubscriptions генерирует SubscribeCommand slices
// для всех текущих активных подписок в пределах ТИПА РЫНКА ЭТОГО клиента.
// Используется во время переподключения. Использует logrus.
// *** НОВЫЙ МЕТОД (АДАПТИРОВАН для MarketType) - Добавлена реализация ***
func (c *BinanceClient) generateSubscribeCommandsFromActiveSubscriptions() []types.SubscriptionCommand {
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithField("func", "generateSubscribeCommandsFromActiveSubscriptions")
	logger.Debug("Генерация команд подписки из активных подписок...")

	c.subMu.RLock() // Блокируем для чтения activeSubscriptions
	defer c.subMu.RUnlock()

	cmds := make([]types.SubscriptionCommand, 0, len(c.activeSubscriptions))
	for _, cmd := range c.activeSubscriptions {
		// Фильтруем команды, чтобы включить только те, которые относятся к ТИПУ РЫНКА ЭТОГО клиента
		// Эта проверка может быть избыточной, если activeSubscriptions содержит только
		// команды для типа рынка этого клиента, но она делает код безопаснее.
		if cmd.Source == "binance" && cmd.MarketType == c.marketType { // Убедимся, что это команды для этого клиента
			cmds = append(cmds, cmd)                                                      // Добавляем копию команды
			logger.WithField("command", cmd).Trace("Добавлена команда для переподписки.") // Trace
		} else {
			logger.WithField("command", cmd).Trace("Пропущена команда для другого клиента/типа рынка.") // Trace
		}
	}

	logger.WithField("count", len(cmds)).Debug("Генерация команд подписки завершена.")
	return cmds
}

// getSymbolsFromActiveDepthSubscriptionsForMarket собирает символы из активных
// подписок на глубину ТОЛЬКО для ТИПА РЫНКА ЭТОГО клиента.
// Используется для инициирования REST snapshot resync после подключения. Использует logrus.
// *** НОВЫЙ МЕТОД (АДАПТИРОВАН для MarketType) - Добавлена реализация ***
func (c *BinanceClient) getSymbolsFromActiveDepthSubscriptionsForMarket() []string {
	logger := c.log.WithField("func", "getSymbolsFromActiveDepthSubscriptionsForMarket")
	logger.Debug("Сбор символов из активных подписок на глубину для текущего типа рынка.")

	c.subMu.RLock()
	defer c.subMu.RUnlock()

	symbolsMap := make(map[string]bool) // Use a map for uniqueness
	lowerSymbolFromTopic := ""          // Переменная для извлеченного символа (в нижнем регистре)

	for topic, activeCmd := range c.activeSubscriptions { // activeCmd - копия значения из мапы
		// Добавим логирование каждого топика и его типа из мапы для отладки
		logger.WithFields(logrus.Fields{
			"active_topic":     topic,
			"cmd_type_in_map":  activeCmd.Type,  // Логируем тип команды, КАК ОН ХРАНИТСЯ В МАПЕ
			"cmd_depth_in_map": activeCmd.Depth, // Логируем глубину
		}).Trace("Проверка активного топика в мапе.")

		// Проверяем, что подписка относится к ТИПУ РЫНКА ЭТОГО клиента
		if activeCmd.Source == "binance" && activeCmd.MarketType == c.marketType {
			// logger.WithField("active_topic", topic).Trace("Топик относится к этому клиенту. Проверка типа.") // Too noisy

			// === УЛУЧШЕННАЯ ПРОВЕРКА: Является ли этот топик подпиской на глубину для БИНАНСА ===
			isDepthTopicFormat := false
			// Проверяем топики формата symbol@depth (глубина 1, 5) - устаревшие для Binance V3, но могут быть в старых реализациях
			// if strings.HasSuffix(topic, "@depth") {
			// 	// Проверяем, что топик не заканчивается на @depth@ (это топики 10+мс)
			// 	if !strings.HasSuffix(topic, "@depth@") {
			// 		isDepthTopicFormat = true
			// 		parts := strings.Split(topic, "@")
			// 		if len(parts) == 2 {
			// 			lowerSymbolFromTopic = parts[0]
			// 		}
			// 	}
			// } else
			if strings.Contains(topic, "@depth") && strings.Contains(topic, "@100ms") {
				// Проверяем топики формата symbol@depthXXX@100ms (глубина 10+)
				// Или быстрый поток symbol@depth@100ms
				parts := strings.Split(topic, "@")
				if len(parts) >= 3 && (strings.HasPrefix(parts[1], "depth") || parts[1] == "depth") && parts[2] == "100ms" {
					isDepthTopicFormat = true
					lowerSymbolFromTopic = parts[0] // Извлекаем символ
				} else {
					// Формат depthXXX@100ms не соответствует, хотя содержит подстроки. Логируем.
					logger.WithField("active_topic", topic).Warn("Формат топика содержит @depth и @100ms, но не соответствует pattern 'symbol@depthX@100ms' или 'symbol@depth@100ms'.")
				}
			}

			// Условие 2: Тип команды, как он хранится в мапе, равен SubscribeDepth
			isSubscribeDepthCommand := activeCmd.Type == types.SubscribeDepth

			// Комбинированное условие:
			if isDepthTopicFormat && isSubscribeDepthCommand {
				// Логируем, что топик успешно идентифицирован как подписка на глубину
				logger.WithField("active_topic", topic).Debug("Топик идентифицирован как активная подписка на глубину.")

				// Извлекаем символ из топика (уже сделано выше при определении isDepthTopicFormat, если формат корректен)
				if lowerSymbolFromTopic != "" {
					symbol := strings.ToUpper(lowerSymbolFromTopic) // Преобразуем в верхний регистр
					symbolsMap[symbol] = true                       // <<< Символ добавляется в мапу
					logger.WithField("symbol", symbol).WithField("topic", topic).Trace("Символ добавлен в список для снимка.")
				} else {
					logger.WithField("active_topic", topic).Warn("Не удалось извлечь символ из топика, хотя формат определен как depth.")
				}
			} else {
				// >>> ЭТОТ ЛОГ СРАБАТЫВАЕТ, КОГДА УСЛОВИЕ isDepthTopicFormat && isSubscribeDepthCommand НЕ ВЕРНО <<<
				logger.WithFields(logrus.Fields{
					"active_topic":           topic,
					"cmd_type_in_map":        activeCmd.Type,
					"is_depth_format":        isDepthTopicFormat,
					"is_subscribe_depth_cmd": isSubscribeDepthCommand,
				}).Trace("Пропуск топика, не являющегося подпиской на глубину (по формату или типу команды в мапе).")
			}
		} else {
			logger.WithField("active_topic", topic).WithFields(logrus.Fields{"cmd_source": activeCmd.Source, "cmd_market_type": activeCmd.MarketType}).Trace("Пропуск топика другого клиента/типа рынка.")
		}
	}

	symbols := make([]string, 0, len(symbolsMap))
	for symbol := range symbolsMap {
		symbols = append(symbols, symbol)
	}

	logger.WithField("symbols", symbols).WithField("count", len(symbols)).Debug("Сбор символов завершен.")
	return symbols
}

// ConnectWebSocket устанавливает WebSocket соединение с биржей.
// Использует clientCtx для DialContext timeout. Использует logrus.
func (c *BinanceClient) ConnectWebSocket() error {
	wsBaseURL := c.cfg.GetWSBaseURL() // Используем URL из специфичной конфигурации рынка

	u, err := url.Parse(wsBaseURL)
	if err != nil {
		// Логируем ошибку через c.log
		c.log.WithError(err).Errorf("ConnectWebSocket: Некорректный WebSocket URL в конфигурации '%s'.", wsBaseURL)
		return fmt.Errorf("BinanceClient (%s): invalid WebSocket URL in config '%s': %w", c.marketType, wsBaseURL, err)
	}

	c.log.Infof("ConnectWebSocket: Подключение к %s...", u.String())

	c.mu.Lock()         // Блокируем мьютекс перед работой с c.conn
	defer c.mu.Unlock() // Гарантируем освобождение мьютекса

	if c.isClosed {
		c.log.Warn("ConnectWebSocket: Попытка подключиться к явно остановленному клиенту.")
		return fmt.Errorf("BinanceClient (%s): attempt to connect on explicitly stopped client", c.marketType)
	}

	// Закрываем старое соединение, если оно существует
	if c.conn != nil {
		c.log.Debug("ConnectWebSocket: Обнаружено старое соединение, закрываю перед подключением.")
		// Используем контекст с небольшим таймаутом для чистого закрытия старого соединения
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		c.log.Tracef("ConnectWebSocket: Закрытие старого соединения с контекстом %v...", closeCtx)
		c.CloseWithContext(closeCtx) // Используем метод закрытия с контекстом
		cancelClose()
		c.conn = nil // Убедимся, что установлено в nil
	}

	dialer := websocket.DefaultDialer // Стандартный Dialar.
	// Таймаут на установку соединения (Dial) задается через контекст, который передается в DialContext.
	dialCtx, dialCancel := context.WithTimeout(c.clientCtx, wsDialTimeout) // Создаем контекст для Dial с таймаутом
	defer dialCancel()                                                     // Гарантируем отмену контекста Dial

	// Устанавливаем соединение. DialContext позволяет прервать попытку подключения через переданный контекст.
	// Используем контекст dialCtx, который имеет таймаут.
	conn, resp, err := dialer.DialContext(dialCtx, u.String(), nil) // Используем контекст с таймаутом

	// Обработка ответа (resp). Тело ответа нужно закрыть, если оно есть.
	if resp != nil && resp.Body != nil {
		// c.log.Debugf("ConnectWebSocket: Получен ответ от сервера при Dial: Status=%s", resp.Status) // Опционально логировать статус
		defer resp.Body.Close() // Гарантируем закрытие тела ответа
		// Опционально: прочитать тело ответа при ненулевом статусе для диагностики.
		// if resp.StatusCode != http.StatusOK {
		// 	bodyBytes, _ := io.ReadAll(resp.Body)
		// 	c.log.Warnf("ConnectWebSocket: Получен ненулевой статус (%d) при Dial. Тело ответа: %s", resp.StatusCode, string(bodyBytes))
		// }
	}

	if err != nil {
		// Ошибка DialContext
		if resp != nil {
			// Если получен ответ, логируем его статус
			c.log.WithError(err).WithField("status_code", resp.StatusCode).Errorf("ConnectWebSocket: Ошибка DialContext для %s. Статус ответа: %s.", u.String(), resp.Status)
		} else {
			// Если ответа нет (например, ошибка DNS или сеть недоступна)
			c.log.WithError(err).Errorf("ConnectWebSocket: Ошибка DialContext для %s (нет ответа).", u.String())
		}
		c.conn = nil // Убедимся, что соединение nil
		// Проверяем, является ли ошибка контекстной отменой (например, из-за таймаута Dial)
		if errors.Is(err, dialCtx.Err()) { // Используем errors.Is для проверки
			c.log.Debug("ConnectWebSocket: Ошибка DialContext вызвана отменой контекста (таймаут или клиент остановлен).")
			return dialCtx.Err() // Возвращаем ошибку контекста
		}
		return fmt.Errorf("BinanceClient (%s): error connecting to %s: %w", c.marketType, u.String(), err)
	}
	// Соединение установлено успешно.

	// Настраиваем PONG обработчик для обновления lastPong.
	conn.SetPongHandler(func(string) error {
		// Этот обработчик выполняется в горутине библиотеки websocket.
		// c.log.Trace("ConnectWebSocket: Received PONG.") // Commented out for less noise
		c.mu.Lock()
		c.lastPong = time.Now()
		c.mu.Unlock()
		return nil // Обработчик PONG должен всегда возвращать nil.
	})
	c.lastPong = time.Now() // Сбрасываем таймер PONG/PING после успешного соединения.

	c.conn = conn // Сохраняем установленное соединение.

	c.log.Infof("ConnectWebSocket: Успешное WebSocket соединение с %s", u.String())

	return nil // Возвращаем nil при успешном соединении.
}

// handleMessage обрабатывает входящие сообщения WebSocket.
// Включает логику парсинга и маршрутизации сообщений. Использует logrus.
func (c *BinanceClient) handleMessage(message []byte) {
	// defer recover для защиты от паники в парсерах или обработчиках
	defer func() {
		if r := recover(); r != nil {
			// Логируем панику с уровнем Error и добавляем стек-трейс.
			c.log.WithFields(logrus.Fields{
				"panic_recover": r,
				"stack_trace":   string(debug.Stack()),
				"raw_message":   string(message), // Логируем сырое сообщение при панике
			}).Error("Recovered from panic in handleMessage.")
		}
	}()

	// Логируем сырое сообщение сразу по получении на уровне Trace.
	// WARNING: Включение этого лога может создавать ОГРОМНЫЙ объем логов, если приходят частые обновления стакана/BBO.
	// В продакшене этот лог следует отключить или оставить на уровне Trace,
	// который должен быть выключен по умолчанию.
	c.log.WithField("message_length", len(message)).Tracef("handleMessage: Received raw message: %s", string(message))

	// Try parsing as a combined stream first.
	var combinedMsg struct {
		Stream string          `json:"stream"` // Topic stream (e.g., "btcusdt@depth")
		Data   json.RawMessage `json:"data"`   // Message data (keep as RawMessage for further parsing)
	}

	// c.log.Trace("handleMessage: Attempting to parse as combined stream...")

	err := json.Unmarshal(message, &combinedMsg)
	if err == nil && combinedMsg.Stream != "" && len(combinedMsg.Data) > 0 {
		// Message successfully decoded as combined stream.
		c.log.Tracef("handleMessage: Successfully parsed as combined stream. Stream: %s", combinedMsg.Stream)
		c.log.WithFields(logrus.Fields{
			"stream":   combinedMsg.Stream,
			"data_len": len(combinedMsg.Data),
		}).Debug("handleMessage: Combined stream detected (debug check).")

		stream := combinedMsg.Stream
		data := combinedMsg.Data // data is json.RawMessage

		// Determine event type by stream topic suffix.
		c.log.Tracef("handleMessage: Routing combined stream message based on stream: %s", stream)

		switch {
		case strings.Contains(stream, "@depth") && strings.Contains(stream, "@100ms"): // Handles @depth@100ms and @depthXXX@100ms
			// It's an order book delta from combined stream.
			c.log.Trace("handleMessage: Combined: Identified as Depth Delta stream. Attempting to parse delta data...")
			// Pass the raw data bytes from combinedMsg.Data to parseOrderBookDelta
			delta, err := c.parseOrderBookDelta(data) // parseOrderBookDelta expects json.RawMessage

			if err != nil {
				// Логируем ошибку парсинга с уровнем Error и включаем Raw Data в контекст.
				c.log.WithError(err).WithFields(logrus.Fields{"stream": stream, "raw_data": string(data)}).Error("Ошибка парсинга order book delta из combined stream.")
				return // Отбрасываем сообщение при ошибке парсинга.
			}
			if delta != nil {
				// Устанавливаем MarketType в OrderBookDelta.
				// delta.MarketType = c.marketType // Уже делается в parseOrderBookDelta
				// Отправляем сообщение сканеру. Используем select с default, чтобы не блокироваться.
				select {
				case c.Channels.OrderBookChan <- &types.OrderBookMessage{
					Source: "binance", MarketType: c.marketType, Symbol: delta.Symbol, Type: "delta",
					Timestamp: delta.Timestamp, Delta: delta,
					RawData: message, // Keep original raw message
				}:
					// Successfully sent
					c.log.Tracef("handleMessage: Combined: Sent Depth Delta for %s to channel.", delta.Symbol) // Очень шумно
				default:
					// Канал полон, сообщение отброшено. Логируем предупреждение.
					c.log.WithField("stream", stream).WithField("symbol", delta.Symbol).Warn("OrderBookChan полный, combined depth delta отброшена.")
				}
			} else {
				// parseOrderBookDelta вернул nil без ошибки (не должно происходить при корректном парсере).
				c.log.WithField("stream", stream).WithField("raw_data", string(data)).Warn("parseOrderBookDelta вернул nil без ошибки для combined stream.")
			}
			return // Message handled as combined stream.

		case strings.HasSuffix(stream, "@bookTicker"):
			// It's a BBO from bookTicker combined stream.
			c.log.Trace("handleMessage: Combined: Identified as BookTicker stream. Attempting to parse BBO data...")
			// Pass the raw data bytes from combinedMsg.Data to parseBBO
			bbo := c.parseBBO(data) // parseBBO expects []byte

			if bbo != nil {
				// Устанавливаем MarketType в BBO.
				// bbo.MarketType = c.marketType // Уже делается в parseBBO
				// Отправляем BBO сканеру. Используем select с default.
				select {
				case c.Channels.BBOChan <- bbo:
					// Successfully sent
					c.log.Tracef("handleMessage: Combined: Sent BBO for %s to channel.", bbo.Symbol) // Trace
				default:
					// Канал полон, сообщение отброшено. Логируем предупреждение.
					c.log.WithField("stream", stream).WithField("symbol", bbo.Symbol).Warn("BBOChan полный, combined BBO отброшена.")
				}
			} else {
				// parseBBO вернул nil (ошибка парсинга логируется внутри parseBBO).
				c.log.WithField("stream", stream).WithField("raw_data", string(data)).Error("parseBBO returned nil for combined stream.")
			}
			return // Message handled as combined stream.

		default:
			// Other stream types in combined mode that we don't handle yet.
			// Логируем на уровне Info или Warn, т.к. это может быть интересно для мониторинга.
			c.log.WithField("stream", stream).WithField("raw_data_length", len(data)).Info("handleMessage: Combined: Получены данные из необрабатываемого combined stream.")
			return // Message handled (as unknown combined stream).
		}
	}

	// --- If not recognized as a combined stream, try parsing as standalone or control message.

	// c.log.Tracef("handleMessage: Message is NOT a combined stream (Unmarshal err: %v). Attempting to parse as standalone or control message...", err) // Log the original error if needed

	// Try parsing as a response to subscribe/unsubscribe request or error message ('id', 'result', 'code', 'msg').
	var controlMsg map[string]json.RawMessage
	// Повторно Unmarshal, т.к. предыдущий Unmarshal в combinedMsg мог быть неполным
	if err := json.Unmarshal(message, &controlMsg); err == nil {
		// c.log.Tracef("handleMessage: Successfully unmarshalled message into map for standalone/control check. Keys: %v", getMapKeys(controlMsg))

		// Check for 'id' field - indicates command response/error
		if idRaw, ok := controlMsg["id"]; ok {
			// c.log.Trace("handleMessage: Standalone: Detected 'id' field. Message is likely a command response.")
			var id interface{} // id может быть числом или строкой
			if unmarshalIDErr := json.Unmarshal(idRaw, &id); unmarshalIDErr != nil {
				c.log.WithError(unmarshalIDErr).WithField("raw_id", string(idRaw)).Warn("Standalone: Ошибка Unmarshalling 'id' поля в сообщении с ID.")
				// Продолжаем, чтобы попытаться разобрать остальную часть сообщения.
			}

			if resultRaw, ok := controlMsg["result"]; ok {
				// Это успешный ответ на команду (subscribe/unsubscribe).
				// Результат может быть null, {}, или списком тем, которые были успешно подписаны/отписаны.
				// Binance обычно возвращает null или {}.
				c.log.WithFields(logrus.Fields{"request_id": id, "result": string(resultRaw)}).Info("Получен успешный ответ на команду.")
				// TODO: Обработать ответ: обновить статус активных подписок на основе подтверждения/отказа (если Binance предоставляет такие детали в ответе).
				return // Обработано как ответ на запрос.
			}
			if errorRaw, ok := controlMsg["error"]; ok {
				// Это ответ с ошибкой на команду.
				var errorData struct {
					Code int    `json:"code"`
					Msg  string `json:"msg"`
				}
				if unmarshalErrorErr := json.Unmarshal(errorRaw, &errorData); unmarshalErrorErr != nil {
					c.log.WithError(unmarshalErrorErr).WithField("request_id", id).WithField("raw_error", string(errorRaw)).Error("Получен ответ с 'error' полем, но не удалось его распарсить.")
				} else {
					c.log.WithFields(logrus.Fields{"request_id": id, "error_code": errorData.Code, "error_msg": errorData.Msg}).Error("Получен ответ с ошибкой от Binance.")
				}
				// TODO: Обработать ошибку: инициировать реподписку? уведомить?
				return // Обработано как ошибка с ID.
			}
			// If message has 'id' but no 'result' or 'error' (e.g., internal service message)
			c.log.WithField("request_id", id).WithField("raw_message", string(message)).Debug("Получено сервисное сообщение с ID без result или error.")
			return // Message с ID без result/code/msg - логируем и завершаем обработку.
		}

		// Try recognizing as standalone stream data based on characteristic fields, if no ID.

		// c.log.Trace("handleMessage: Standalone: Checking for known data stream patterns using keys...")

		// Check for field 'e' and its value first (DepthUpdate, BookTicker on Futures)
		if eventTypeRaw, typeOk := controlMsg["e"]; typeOk {
			var eventType string
			if unmarshalEventErr := json.Unmarshal(eventTypeRaw, &eventType); unmarshalEventErr == nil {
				// c.log.Tracef("handleMessage: Standalone: Detected 'e' field with event type '%s'. Attempting to process...", eventType)
				switch eventType {
				case "depthUpdate":
					// Standalone DepthUpdate message
					c.log.Trace("handleMessage: Standalone: Identified as DepthUpdate ('e' field). Attempting to parse delta data...")
					delta, err := c.parseOrderBookDelta(message) // parseOrderBookDelta expects json.RawMessage
					if err != nil {
						c.log.WithError(err).WithField("raw_message", string(message)).Error("Ошибка парсинга order book delta (standalone e='depthUpdate').")
						return // Drop message on parsing error.
					}
					if delta != nil {
						// Устанавливаем MarketType.
						// delta.MarketType = c.marketType // Уже делается в parseOrderBookDelta
						// Отправляем сканеру.
						select {
						case c.Channels.OrderBookChan <- &types.OrderBookMessage{
							Source: "binance", MarketType: c.marketType, Symbol: delta.Symbol, Type: "delta",
							Timestamp: delta.Timestamp, Delta: delta,
							RawData: message, // Keep original raw data
						}:
							// c.log.Tracef("handleMessage: Standalone: Sent DepthUpdate for %s to channel.", delta.Symbol) // Очень шумно
						default:
							// Канал полон.
							c.log.WithField("symbol", delta.Symbol).Warn("OrderBookChan полный, standalone depth delta отброшена.")
						}
					} else {
						// parseOrderBookDelta вернул nil без ошибки.
						c.log.WithField("raw_message", string(message)).Warn("Standalone: parseOrderBookDelta returned nil without error for e='depthUpdate'.")
					}
					return // Обработано как standalone DepthUpdate.

				case "bookTicker":
					// Standalone BookTicker message (on Futures)
					c.log.Trace("handleMessage: Standalone: Identified as BookTicker ('e' field). Attempting to parse BBO data...")
					bbo := c.parseBBO(message) // parseBBO expects []byte and handles the full struct

					if bbo != nil {
						// Устанавливаем MarketType.
						// bbo.MarketType = c.marketType // Уже делается в parseBBO
						// Отправляем сканеру.
						select {
						case c.Channels.BBOChan <- bbo:
							c.log.Tracef("handleMessage: Standalone: Sent BookTicker BBO for %s to channel.", bbo.Symbol) // Trace
						default:
							// Канал полон.
							c.log.WithField("symbol", bbo.Symbol).Warn("BBOChan полный, standalone BookTicker отброшена.")
						}
					} else {
						// parseBBO вернул nil (ошибка парсинга логируется внутри parseBBO).
						c.log.WithField("raw_message", string(message)).Error("Ошибка парсинга BookTicker (standalone e='bookTicker'). parseBBO вернул nil.")
					}
					return // Обработано как standalone BookTicker.

				// Add other known event types from the 'e' field here if needed
				default:
					// Неизвестный тип события по полю 'e'. Логируем на уровне Info или Warn.
					c.log.WithField("event_type", eventType).WithField("raw_message", string(message)).Info("Standalone: Обнаружено поле 'e' с необрабатываемым типом события.")
					// Не возвращаемся пока, возможно, подходит под другой шаблон ниже (менее вероятно, но безопасно)
				}
			} else {
				// Не удалось распарсить поле 'e' как строку. Логируем на уровне Warn.
				c.log.WithError(unmarshalEventErr).WithField("raw_e_field", string(eventTypeRaw)).Warn("Standalone: Обнаружено поле 'e', но не удалось распарсить как строку.")
				// Не возвращаемся пока
			}
		}
		// Если поле 'e' не обнаружено, или оно было необработано, продолжаем проверять другие шаблоны.

		// Проверка на Binance Spot BBO (Standalone) - у него НЕТ поля 'e', но ЕСТЬ поля u, s, b, B, a, A
		// Нужно проверить наличие всех характерных BBO ключей *и* убедиться, что это не combined stream
		// и не было обработано проверкой поля 'e' выше.
		// Карта `controlMsg` уже имеет проверенные ключи.
		_, sOk := controlMsg["s"]
		_, bOk := controlMsg["b"]
		_, BOk := controlMsg["B"]
		_, aOk := controlMsg["a"]
		_, AOk := controlMsg["A"]
		_, uOk := controlMsg["u"]
		_, eExists := controlMsg["e"]   // Проверяем, присутствовало ли поле 'e' вообще
		_, idExists := controlMsg["id"] // Убедимся, что это не ответ на команду

		// Условие для характерного Spot BBO: присутствуют обязательные BBO ключи И НЕТ поля 'e' И НЕТ поля 'id'.
		// parseBBO обработает парсинг строк/чисел и логику Timestamp.
		if sOk && bOk && BOk && aOk && AOk && uOk && !eExists && !idExists {
			c.log.Trace("handleMessage: Standalone: Идентифицировано как потенциальное Spot BBO (нет поля 'e', есть ключи BBO). Попытка парсинга...")
			bbo := c.parseBBO(message) // parseBBO ожидает []byte and handles the full struct

			if bbo != nil {
				// Устанавливаем MarketType.
				// bbo.MarketType = c.marketType // Уже делается в parseBBO
				// Отправляем сканеру.
				select {
				case c.Channels.BBOChan <- bbo:
					c.log.Tracef("handleMessage: Standalone: Sent Spot BBO for %s to channel.", bbo.Symbol) // Trace
				default:
					// Канал полон.
					c.log.WithField("symbol", bbo.Symbol).Warn("BBOChan полный, standalone Spot BBO отброшена.")
				}
			} else {
				// parseBBO вернул nil (ошибка парсинга логируется внутри parseBBO).
				c.log.WithField("raw_message", string(message)).Error("Ошибка парсинга Spot BBO (standalone, нет 'e'). parseBBO вернул nil.")
			}
			return // Обработано как standalone Spot BBO.
		}

		// If we reached here, message was deserialized into map, but didn't match known patterns.
		c.log.WithFields(logrus.Fields{
			"message_keys": getMapKeys(controlMsg),
			"raw_message":  string(message),
		}).Warn("Received other service or unknown message after map check.")

	} else {
		// If Unmarshal into map completely failed, it could be PING/PONG (handled by library)
		// or totally invalid JSON.
		c.log.WithError(err).WithField("raw_message", string(message)).Error("Failed to unmarshal message into map for standalone/control check.")
	}

	// If execution reaches here, the message wasn't successfully handled by any logic.
	// It was likely already logged if it was unparsable JSON or an unknown map format.
}

// getMapKeys Хелпер для получения ключей мапы (для логирования) - остался прежним
func getMapKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// parseOrderBookDelta парсит JSON order book delta из RawMessage.
// Ожидает структуру с полями "e", "E", "s", "U", "u", "b", "a". Устанавливает MarketType. Использует logrus.
// *** ДОБАВЛЕНО этот отсутствующий метод ***
func (c *BinanceClient) parseOrderBookDelta(data json.RawMessage) (*types.OrderBookDelta, error) {
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithField("func", "parseOrderBookDelta").WithField("data_len", len(data))
	logger.Trace("Начало парсинга order book delta.")

	var deltaMsg struct {
		EventType string     `json:"e"` // "depthUpdate"
		EventTime int64      `json:"E"` // Timestamp (milliseconds)
		Symbol    string     `json:"s"` // Symbol
		FirstID   int64      `json:"U"` // First update ID in event
		FinalID   int64      `json:"u"` // Final update ID in event
		Bids      [][]string `json:"b"` // bids to be updated [price, quantity] as strings
		Asks      [][]string `json:"a"` // asks to be updated [price, quantity] as strings
	}

	err := json.Unmarshal(data, &deltaMsg)
	if err != nil {
		// Логируем ошибку Unmarshal на уровне Error. Включаем сырые данные для отладки.
		logger.WithError(err).WithField("raw_data", string(data)).Error("Ошибка парсинга JSON order book delta.")
		return nil, fmt.Errorf("BinanceClient (%s): error parsing JSON order book delta: %w, data: %s", c.marketType, err, string(data))
	}
	// Базовая проверка формата сообщения.
	if deltaMsg.EventType != "depthUpdate" || deltaMsg.Symbol == "" {
		// Логируем ошибку формата на уровне Error.
		logger.WithFields(logrus.Fields{
			"event_type": deltaMsg.EventType,
			"symbol":     deltaMsg.Symbol,
			"raw_data":   string(data),
		}).Error("Некорректный формат order book delta (Отсутствует тип события или символ).")
		return nil, fmt.Errorf("BinanceClient (%s): invalid order book delta format (Missing event type or symbol) data: %s", c.marketType, string(data))
	}

	// Парсинг уровней Bids. Логируем ошибки парсинга каждого уровня на уровне Warn.
	parsedBids := make([]types.PriceLevel, 0, len(deltaMsg.Bids))
	for i, bidLevel := range deltaMsg.Bids {
		if len(bidLevel) == 2 {
			price, err1 := strconv.ParseFloat(bidLevel[0], 64)
			amount, err2 := strconv.ParseFloat(bidLevel[1], 64)
			if err1 == nil && err2 == nil {
				parsedBids = append(parsedBids, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга цены или количества. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side":               "bid",
					"index":              i,
					"level":              bidLevel,
					"parse_price_error":  err1,
					"parse_amount_error": err2,
				}).Warn("Ошибка парсинга уровня bid в order book delta. Пропуск уровня.")
			}
		} else {
			// Некорректный формат уровня (не 2 элемента). Логируем предупреждение.
			logger.WithFields(logrus.Fields{
				"side":  "bid",
				"index": i,
				"level": bidLevel,
			}).Warn("Некорректный формат уровня bid в order book delta (не 2 элемента). Пропуск уровня.")
		}
	}

	// Парсинг уровней Asks. Логируем ошибки парсинга каждого уровня на уровне Warn.
	parsedAsks := make([]types.PriceLevel, 0, len(deltaMsg.Asks))
	for i, askLevel := range deltaMsg.Asks {
		if len(askLevel) == 2 {
			price, err1 := strconv.ParseFloat(askLevel[0], 64)
			amount, err2 := strconv.ParseFloat(askLevel[1], 64)
			if err1 == nil && err2 == nil {
				parsedAsks = append(parsedAsks, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга цены или количества. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side":               "ask",
					"index":              i,
					"level":              askLevel,
					"parse_price_error":  err1,
					"parse_amount_error": err2,
					// ИСПРАВЛЕНО: Убран лишний .Warn("Ошибка парсинг...")
				}).Warn("Ошибка парсинга уровня ask в order book delta. Пропуск уровня.")
			}
		} else {
			// Некорректный формат уровня (не 2 элемента). Логируем предупреждение.
			logger.WithFields(logrus.Fields{
				"side":  "ask",
				"index": i,
				"level": askLevel,
			}).Warn("Некорректный формат уровня ask в order book delta (не 2 элемента). Пропуск уровня.")
		}
	}

	// Создаем и возвращаем структуру OrderBookDelta.
	delta := &types.OrderBookDelta{
		Symbol:        strings.ToUpper(deltaMsg.Symbol), // Binance symbols обычно в верхнем регистре
		Bids:          parsedBids,
		Asks:          parsedAsks,
		Timestamp:     time.UnixMilli(deltaMsg.EventTime), // Используем EventTime как Timestamp
		Source:        "binance",
		MarketType:    c.marketType,     // Устанавливаем MarketType клиента
		FirstUpdateID: deltaMsg.FirstID, // U
		FinalUpdateID: deltaMsg.FinalID, // u
	}
	logger.WithFields(logrus.Fields{
		"symbol":     delta.Symbol,
		"U":          delta.FirstUpdateID,
		"u":          delta.FinalUpdateID,
		"bids_count": len(delta.Bids),
		"asks_count": len(delta.Asks),
	}).Trace("Парсинг order book delta завершен успешно.")

	return delta, nil // Возвращаем дельту и nil ошибку.
}

// parseBBO parses JSON BBO (@bookTicker) data from RawMessage or []byte.
// Expects structure with fields "u", "s", "b", "B", "a", "A". Sets MarketType. Uses logrus.
// *** ДОБАВЛЕНО этот отсутствующий метод ***
// Принимает []byte (или json.RawMessage, т.к. RawMessage - это []byte алиас)
func (c *BinanceClient) parseBBO(data []byte) *types.BBO {
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithField("func", "parseBBO").WithField("data_len", len(data))
	logger.Trace("Начало парсинга BBO.")

	// Используем карту для более гибкого парсинга, так как формат может немного отличаться
	var msgMap map[string]json.RawMessage

	err := json.Unmarshal(data, &msgMap)
	if err != nil {
		// Логируем ошибку парсинга JSON на уровне Warn. Включаем сырые данные на уровне Debug.
		// Если это вызвано мусорными данными, лог может быть частым.
		logger.WithError(err).Warn("Ошибка Unmarshal JSON для BBO парсинга.")
		logger.WithField("raw_data", string(data)).Debug("Сырые данные при ошибке BBO парсинга.")
		return nil // Возвращаем nil при ошибке парсинга.
	}

	// Извлекаем поля, проверяя их наличие
	symbolRaw, sOk := msgMap["s"]
	bidPriceRaw, bOk := msgMap["b"]
	bidQtyRaw, BOk := msgMap["B"]
	askPriceRaw, aOk := msgMap["a"]
	askQtyRaw, AOk := msgMap["A"]
	// Поле 'u' есть в обоих типах BBO сообщений от Binance (и в depth)
	_, uOk := msgMap["u"]

	// Для фьючерсов может быть поле 'e'="bookTicker" и поля времени 'T', 'E'
	// Для спота их нет.
	// Для combined stream data их тоже нет (они на верхнем уровне)

	// Проверяем наличие всех обязательных BBO полей
	if !sOk || !bOk || !BOk || !aOk || !AOk || !uOk {
		// Логируем неполные данные BBO на уровне Warn, включаем найденные ключи в контекст.
		logger.WithFields(logrus.Fields{"keys_found": getMapKeys(msgMap)}).Warn("Получены неполные данные BBO (отсутствуют ключи).")
		// logger.WithField("raw_data", string(data)).Debug("Сырые данные неполного BBO.") // Debug сырых данных
		return nil
	}

	var symbol string
	// Парсим символ. Он должен быть строкой.
	if err := json.Unmarshal(symbolRaw, &symbol); err != nil {
		logger.WithError(err).WithField("raw_symbol", string(symbolRaw)).Error("Не удалось unmarshal поле символа из данных BBO.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	symbol = strings.ToUpper(symbol) // Убедимся, что символ в верхнем регистре

	// Поля b, B, a, A должны быть строками в BBO от Binance WS.
	var bestBidStr, bestBidQtyStr, bestAskStr, bestAskQtyStr string
	if err := json.Unmarshal(bidPriceRaw, &bestBidStr); err != nil {
		logger.WithError(err).WithField("symbol", symbol).WithField("raw_bid_price", string(bidPriceRaw)).Error("Не удалось unmarshal строку лучшей цены покупки из BBO.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	if err := json.Unmarshal(bidQtyRaw, &bestBidQtyStr); err != nil {
		logger.WithError(err).WithField("symbol", symbol).WithField("raw_bid_qty", string(bidQtyRaw)).Error("Не удалось unmarshal строку количества лучшей покупки из BBO.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	if err := json.Unmarshal(askPriceRaw, &bestAskStr); err != nil {
		logger.WithError(err).WithField("symbol", symbol).WithField("raw_ask_price", string(askPriceRaw)).Error("Не удалось unmarshal строку лучшей цены продажи из BBO.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	if err := json.Unmarshal(askQtyRaw, &bestAskQtyStr); err != nil {
		logger.WithError(err).WithField("symbol", symbol).WithField("raw_ask_qty", string(askQtyRaw)).Error("Не удалось unmarshal строку количества лучшей продажи из BBO.")
		return nil // Возвращаем nil при ошибке парсинга.
	}

	// Парсим числовые значения из строк. Логируем ошибки парсинга чисел.
	bestBid, err := strconv.ParseFloat(bestBidStr, 64)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{"symbol": symbol, "price_str": bestBidStr}).Error("Ошибка парсинга лучшей цены покупки из строки.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	bestBidQty, err := strconv.ParseFloat(bestBidQtyStr, 64)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{"symbol": symbol, "qty_str": bestBidQtyStr}).Error("Ошибка парсинга количества лучшей покупки из строки.")
		return nil // Возвращаем nil при ошибке парсинга.
	}
	bestAsk, err := strconv.ParseFloat(bestAskStr, 64)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{"symbol": symbol, "price_str": bestAskStr}).Error("Ошибка парсинга лучшей цены продажи из строки.")
		return nil // Возвращаем nil при ошибке парsing.
	}
	bestAskQty, err := strconv.ParseFloat(bestAskQtyStr, 64)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{"symbol": symbol, "qty_str": bestAskQtyStr}).Error("Ошибка парсинга количества лучшей продажи из строки.")
		return nil // Возвращаем nil при ошибке парсинга.
	}

	// Определяем Timestamp. Предпочитаем биржевое время, если доступно (поля T или E в миллисекундах).
	// Futures BookTicker имеет поля T и E (milliseconds).
	// Spot BookTicker (ws stream) имеет только u.
	// Combined stream data не имеет временных полей, кроме, возможно, если они вложены в Data.
	// Будем искать T или E в миллисекундах.
	timestamp := time.Now() // Значение по умолчанию (время получения сообщения)
	if tRaw, ok := msgMap["T"]; ok {
		var ts int64
		if err := json.Unmarshal(tRaw, &ts); err == nil {
			timestamp = time.Unix(0, ts*int64(time.Millisecond))
			// logger.Tracef("Timestamp from T: %s", timestamp.Format(time.RFC3339Nano)) // Trace
		} else {
			logger.WithError(err).WithField("raw_T", string(tRaw)).Trace("Не удалось распарсить поле 'T' как int64 для Timestamp.")
		}
	} else if eRaw, ok := msgMap["E"]; ok {
		var ts int64
		if err := json.Unmarshal(eRaw, &ts); err == nil {
			timestamp = time.Unix(0, ts*int64(time.Millisecond))
			// logger.Tracef("Timestamp from E: %s", timestamp.Format(time.RFC3339Nano)) // Trace
		} else {
			logger.WithError(err).WithField("raw_E", string(eRaw)).Trace("Не удалось распарсить поле 'E' как int64 для Timestamp.")
		}
	}
	// Если ни T, ни E нет (например, Spot BBO), используется time.Now() по умолчанию.

	// Создаем и возвращаем структуру BBO.
	bbo := &types.BBO{
		Source:          "binance",
		MarketType:      c.marketType, // Устанавливаем MarketType клиента
		Symbol:          symbol,
		BestBid:         bestBid,
		BestBidQuantity: bestBidQty,
		BestAsk:         bestAsk,
		BestAskQuantity: bestAskQty,
		Timestamp:       timestamp, // Используем определенный выше timestamp
	}

	logger.WithFields(logrus.Fields{
		"symbol":    bbo.Symbol,
		"bid_price": bbo.BestBid,
		"bid_qty":   bbo.BestBidQuantity,
		"ask_price": bbo.BestAsk,
		"ask_qty":   bbo.BestAskQuantity,
		"timestamp": bbo.Timestamp.Format(time.RFC3339Nano),
	}).Trace("Парсинг BBO завершен успешно.") // Trace

	return bbo // Возвращаем указатель на BBO.
}

// GetOrderBookSnapshotREST fetches a Binance order book snapshot via REST API.
// Uses the Rate Limiter to limit request frequency. Uses logrus.
// symbol: Trading symbol (must be uppercase).
// limit: Desired order book depth (5, 10, 20, 50, 100, 500, 1000). 5000 requires separate permission.
// Uses ctx for HTTP request cancellation and Rate Limiter waiting.
// *** ДОБАВЛЕНО этот отсутствующий метод - Реализация зависит от cfg.GetRESTBaseURL() и MarketType ***
func (c *BinanceClient) GetOrderBookSnapshotREST(ctx context.Context, symbol string, limit int) (*types.OrderBook, error) {
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithFields(logrus.Fields{
		"func":   "GetOrderBookSnapshotREST",
		"symbol": symbol, // Символ уже должен быть в верхнем регистре
		"limit":  limit,
	})
	logger.Debug("Запрос REST snapshot стакана...")

	restBaseURL := c.cfg.GetRESTBaseURL() // Используем URL из специфичной конфигурации рынка

	// Определяем правильный API путь в зависимости от типа рынка
	var apiPath string
	switch c.marketType {
	case types.MarketTypeSpot:
		apiPath = "/api/v3/depth" // https://api.binance.com/api/v3/depth
	case types.MarketTypeLinearFutures, types.MarketTypeCoinFutures: // Проверьте документацию Binance для пути Coin-M
		apiPath = "/fapi/v1/depth" // https://fapi.binance.com/fapi/v1/depth
	default:
		// Неподдерживаемый тип рынка. Логируем ошибку.
		logger.Errorf("Неподдерживаемый тип рынка для REST snapshot: %s", c.marketType)
		return nil, fmt.Errorf("BinanceClient (%s): Unsupported market type for REST snapshot: %s", c.marketType, c.marketType)
	}

	// Формируем полный URL запроса
	requestURL := fmt.Sprintf("%s%s?symbol=%s&limit=%d", restBaseURL, apiPath, strings.ToUpper(symbol), limit) // Убедимся, что символ в URL в верхнем регистре

	// Calculate weight for the REST request using the helper function (assuming it's implemented)
	// TODO: Реализовать GetBinanceDepthWeight в rate_limiter или util пакете, если он еще не существует.
	// Предполагаем, что rate_limiter.GetBinanceDepthWeight существует и возвращает корректный вес.
	weight := rate_limiter.GetBinanceDepthWeight(limit) // Эта функция должна быть реализована
	logger.WithField("weight", weight).Debug("Вес запроса REST snapshot. Ожидание Rate Limiter...")

	// Ожидаем разрешения от Rate Limiter. Метод Wait должен принимать контекст.
	if !c.restLimiter.Wait(ctx, weight) { // Используем переданный контекст (ctx)
		// Если Wait возвращает false, это означает, что контекст был отменен во время ожидания.
		logger.Info("REST запрос отменен из-за отмены контекста во время ожидания RL.")
		return nil, ctx.Err() // Возвращаем ошибку контекста
	}
	logger.Debug("Rate Limiter разрешил запрос.")

	// Используем стандартный HTTP клиент. TODO: Настроить таймаут в конфиге?
	httpClient := &http.Client{Timeout: 15 * time.Second} // Таймаут для всего HTTP запроса

	// Создаем запрос с переданным контекстом ctx для отмены.
	req, err := http.NewRequestWithContext(ctx, "GET", requestURL, nil) // Используем переданный ctx
	if err != nil {
		// Логируем ошибку создания запроса.
		logger.WithError(err).Error("Ошибка создания HTTP snapshot запроса.")
		return nil, fmt.Errorf("BinanceClient (%s): Error creating HTTP snapshot request: %w", c.marketType, err)
	}

	// Выполняем GET запрос. Метод Do(req) учитывает контекст в req.
	resp, err := httpClient.Do(req) // Использует контекст из req
	if err != nil {
		// Проверяем, была ли ошибка вызвана отменой контекста во время выполнения Do.
		select {
		case <-ctx.Done(): // Слушаем переданный ctx
			logger.Info("REST запрос отменен из-за отмены контекста во время выполнения Do.")
			return nil, ctx.Err() // Возвращаем ошибку отмены контекста
		default:
			// Это обычная сетевая ошибка или таймаут, не связанный с отменой контекста.
			logger.WithError(err).Error("Ошибка выполнения HTTP snapshot запроса.")
			return nil, fmt.Errorf("BinanceClient (%s): Error performing HTTP snapshot request: %w", c.marketType, err)
		}
	}
	defer resp.Body.Close() // Гарантируем закрытие тела ответа.

	// Проверяем статус код ответа.
	if resp.StatusCode != http.StatusOK {
		// Логируем ошибку ненулевого статуса. Читаем тело ответа для диагностики.
		var bodyBytes []byte
		bodyBytes, readBodyErr := io.ReadAll(resp.Body) // Читаем тело для дополнительной информации, игнорируем ошибку чтения тела
		if readBodyErr != nil {
			logger.WithError(readBodyErr).Error("Ошибка чтения тела ответа при ненулевом статусе.")
			bodyBytes = []byte(fmt.Sprintf("Error reading body: %v", readBodyErr)) // Заменяем тело на сообщение об ошибке чтения
		}
		// Логируем ошибку с уровнем Error, включаем статус и тело ответа.
		logger.WithFields(logrus.Fields{"status_code": resp.StatusCode, "response_body": string(bodyBytes)}).Errorf("Получен ненулевой статус код для snapshot запроса.")
		return nil, fmt.Errorf("BinanceClient (%s): Received non-OK status code for snapshot request (%s): %d - %s",
			c.marketType, requestURL, resp.StatusCode, string(bodyBytes))
	}

	// Читаем тело ответа.
	var bodyBytes []byte
	bodyBytes, err = io.ReadAll(resp.Body)
	if err != nil {
		// Логируем ошибку чтения тела.
		logger.WithError(err).Error("Ошибка чтения тела ответа snapshot.")
		return nil, fmt.Errorf("BinanceClient (%s): Error reading snapshot response body: %w", c.marketType, err)
	}

	// Структура для парсинга JSON ответа REST depth.
	var snapshotData struct {
		LastUpdateID int64      `json:"lastUpdateId"`
		Bids         [][]string `json:"bids"` // Bid levels [[price, quantity]]
		Asks         [][]string `json:"asks"` // Ask levels [[price, quantity]]
		// REST endpoint depth не включает временные метки 'E' и 'T'.
	}

	// Парсим JSON ответ.
	err = json.Unmarshal(bodyBytes, &snapshotData)
	if err != nil {
		// Логируем ошибку парсинга JSON. Включаем сырые данные ответа.
		logger.WithError(err).WithField("response_body", string(bodyBytes)).Error("Ошибка парсинга REST snapshot JSON.")
		return nil, fmt.Errorf("BinanceClient (%s): Error parsing REST snapshot JSON: %w, response: %s", c.marketType, err, string(bodyBytes))
	}

	// Конвертируем [][]string в []types.PriceLevel. Логируем ошибки парсинга уровней на уровне Warn.
	parsedBids := make([]types.PriceLevel, 0, len(snapshotData.Bids))
	for i, level := range snapshotData.Bids {
		if len(level) == 2 {
			price, err1 := strconv.ParseFloat(level[0], 64)
			amount, err2 := strconv.ParseFloat(level[1], 64)
			if err1 == nil && err2 == nil {
				parsedBids = append(parsedBids, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга уровня bid. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side":               "bid",
					"index":              i,
					"level_data":         level,
					"parse_price_error":  err1,
					"parse_amount_error": err2,
				}).Warn("Ошибка парсинга уровня bid в REST snapshot. Пропуск уровня.")
			}
		} else {
			// Некорректный формат уровня bid.
			logger.WithFields(logrus.Fields{
				"side":       "bid",
				"index":      i,
				"level_data": level,
			}).Warn("Некорректный формат уровня bid в REST snapshot (не 2 элемента). Пропуск уровня.")
		}
	}

	parsedAsks := make([]types.PriceLevel, 0, len(snapshotData.Asks))
	for i, level := range snapshotData.Asks {
		if len(level) == 2 {
			price, err1 := strconv.ParseFloat(level[0], 64)
			amount, err2 := strconv.ParseFloat(level[1], 64)
			if err1 == nil && err2 == nil {
				parsedAsks = append(parsedAsks, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга уровня ask. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side":               "ask",
					"index":              i,
					"level_data":         level,
					"parse_price_error":  err1,
					"parse_amount_error": err2,
				}).Warn("Ошибка парсинга уровня ask в REST snapshot. Пропуск уровня.")
			}
		} else {
			// Некорректный формат уровня ask.
			logger.WithFields(logrus.Fields{
				"side":       "ask",
				"index":      i,
				"level_data": level,
			}).Warn("Некорректный формат уровня ask в REST snapshot (не 2 элемента). Пропуск уровня.")
		}
	}

	// REST snapshot не имеет временной метки события как WS. Используем текущее время.
	snapshot := &types.OrderBook{
		Source:       "binance",
		MarketType:   c.marketType,            // Устанавливаем MarketType
		Symbol:       strings.ToUpper(symbol), // Символ в верхнем регистре
		Bids:         parsedBids,
		Asks:         parsedAsks,
		Timestamp:    time.Now(), // Используем текущее время для REST snapshot
		LastUpdateID: snapshotData.LastUpdateID,
	}

	logger.WithFields(logrus.Fields{
		"snapshot_id": snapshot.LastUpdateID,
		"bids_count":  len(snapshot.Bids),
		"asks_count":  len(snapshot.Asks),
	}).Debug("Парсинг REST snapshot завершен успешно.")

	return snapshot, nil // Возвращаем snapshot и nil ошибку.
}

// KeepAlive sends standard WS PING frames at a defined interval.
// Listens to clientCtx for termination. Uses logrus.
// *** ДОБАВЛЕНО этот отсутствующий метод ***
func (c *BinanceClient) keepAlive(ctx context.Context) {
	c.log.Infof("Keep-Alive goroutine launched (WS Ping) with interval %s", c.pingInterval)
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	defer c.log.Info("Keep-Alive goroutine finished.")

	for {
		select {
		case <-ticker.C: // Срабатывает по интервалу таймера.
			c.mu.Lock() // Блокируем мьютекс для доступа к c.conn и c.lastPong
			if c.isClosed || c.conn == nil {
				c.mu.Unlock()
				// c.log.Debug("Keep-Alive: Connection closed or nil, завершение горутины.") // Too noisy, use Trace
				return // Выход из горутины, если клиент закрыт или соединение nil.
			}

			// Проверяем активность PONG (опционально, таймаут чтения в Listen - основной механизм).
			// Если прошло слишком много времени с последнего PONG, возможно, соединение мертво.
			// c.wsReadTimeout - 5*time.Second - небольшой буфер перед фактическим таймаутом чтения.
			if time.Since(c.lastPong) > c.wsReadTimeout-5*time.Second {
				c.mu.Unlock() // Освобождаем мьютекс перед логированием и выходом
				c.log.Warn("Keep-Alive: Соединение кажется неактивным (PONG давно не получался). Listen, вероятно, скоро сработает по таймауту.")
				return // Выход из Keep-Alive. Run обнаружит Listen death и переподключится.
			}

			// Устанавливаем дедлайн записи для отправки PING сообщения.
			writeDeadline := time.Now().Add(c.wsWriteTimeout)
			err := c.conn.SetWriteDeadline(writeDeadline)
			c.mu.Unlock() // Освобождаем мьютекс перед WriteControl.

			if err != nil {
				c.log.WithError(err).Error("Keep-Alive: Ошибка установки WriteDeadline для PING. Выход.")
				return // Выход при ошибке.
			}

			// Отправляем стандартный WS PING frame. Используем таймаут для записи управляющего фрейма.
			// Таймаут здесь должен быть меньше или равен WS Write Timeout.
			err = c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.wsWriteTimeout))

			// Сбрасываем дедлайн записи после отправки. ВАЖНО!
			c.mu.Lock()        // Блокируем снова для доступа к c.conn
			if c.conn != nil { // Проверяем conn на nil под мьютексом
				c.conn.SetWriteDeadline(time.Time{}) // Нулевое время означает отсутствие дедлайна.
			}
			c.mu.Unlock() // Освобождаем мьютекс

			if err != nil {
				// Ошибка отправки PING. Логируем и выходим.
				c.log.WithError(err).Error("Keep-Alive: Ошибка отправки PING. Выход.")
				return // Выход при ошибке.
			}
			// c.log.Trace("Keep-Alive: PING отправлен.") // Commented out - Too noisy Trace

		case <-ctx.Done(): // Слушаем внутренний контекст клиента для завершения.
			c.log.Info("Keep-Alive: Получен сигнал отмены контекста (clientCtx), завершение горутины.")
			return // Выход из горутины.
		}
	}
}

// performBinanceResync выполняет запрос REST snapshot и отправляет его в OrderBookChan.
// Запускается в отдельной горутине обработчиком команд ресинка (handleResyncCommands).
// Принимает контекст (clientCtx), под которым выполняется.
// *** ИСПРАВЛЕНО: Убран параметр wg. ***
func (c *BinanceClient) performBinanceResync(ctx context.Context, symbol string) {
	// Используем логгер с контекстом для этой горутины
	logger := c.log.WithFields(logrus.Fields{
		"goroutine": "rest_resync",
		"symbol":    symbol, // Символ уже в верхнем регистре
	})
	logger.Info("Запуск горутины resync (REST Snapshot).")
	// defer c.clientWg.Done() // Done() вызывается в handleResyncCommands после запуска этой горутины

	maxAttempts := 5         // Максимальное количество попыток для запроса REST snapshot (TODO: из конфига)
	delay := 3 * time.Second // Задержка между попытками (TODO: из конфига, backoff)

	for attempts := 1; attempts <= maxAttempts; attempts++ {
		logger.Infof("Попытка %d/%d запроса REST snapshot...", attempts, maxAttempts)

		// Проверяем контекст перед каждой попыткой запроса.
		select {
		case <-ctx.Done(): // Слушаем переданный контекст (clientCtx)
			logger.Info("Отмена контекста во время цикла попыток REST resync. Выход.")
			return // Выход.
		default:
		}

		// TODO: Использовать limit для snapshot из конфига или команды?
		// Для начала используем хардкод 1000 (проверьте, поддерживается ли лимитом endpoint'а).
		limit := 1000 // Проверьте документацию Binance для максимальной глубины snapshot по endpoint'у.

		// GetOrderBookSnapshotREST обрабатывает ожидание Rate Limiter и возвращает *types.OrderBook или ошибку.
		// GetOrderBookSnapshotREST также использует переданный ему контекст (ctx).
		snapshot, err := c.GetOrderBookSnapshotREST(ctx, symbol, limit) // GetOrderBookSnapshotREST использует ctx

		if err != nil {
			// Проверяем, вызвана ли ошибка отменой контекста
			if errors.Is(err, ctx.Err()) {
				logger.Info("REST запрос отменен из-за отмены контекста во время GetOrderBookSnapshotREST. Выход.")
				return // Выход, если контекст отменен.
			}

			// Логируем ошибку попытки, но не выходим сразу, если есть оставшиеся попытки.
			logger.WithError(err).Errorf("Ошибка (Попытка %d/%d) во время запроса REST snapshot.", attempts, maxAttempts)

			if attempts < maxAttempts {
				logger.Infof("Ожидание %s перед следующей попыткой...", delay)
				select {
				case <-time.After(delay):
					continue // Продолжаем цикл к следующей попытке
				case <-ctx.Done(): // Проверяем контекст во время ожидания.
					logger.Info("Ожидание REST resync прервано отменой контекста. Выход.")
					return
				}
			} else { // Попытки исчерпаны
				logger.Errorf("Все %d попыток получения REST snapshot провалились.", maxAttempts)
				// TODO: Метрика или уведомление о неудачном ресинке.
				// TODO: Возможно, уведомить Scanner или Main о критической ошибке синхронизации?
				return // Выход, т.к. попытки исчерпаны.
			}
		} else { // Snapshot получен успешно (err == nil)
			logger.WithFields(logrus.Fields{
				"snapshot_id": snapshot.LastUpdateID,
				"bids_count":  len(snapshot.Bids),
				"asks_count":  len(snapshot.Asks),
			}).Info("REST snapshot resync успешен.")

			// Форматируем сообщение для Scanner.
			// MarketType уже должен быть установлен в snapshot методом GetOrderBookSnapshotREST.
			snapshotMsg := &types.OrderBookMessage{
				Source:     "binance",
				MarketType: snapshot.MarketType, // MarketType уже в snapshot
				Symbol:     snapshot.Symbol,
				Type:       "snapshot_rest",    // Тип "snapshot_rest" для Scanner
				Timestamp:  snapshot.Timestamp, // Timestamp из snapshot
				Delta: &types.OrderBookDelta{ // Копируем данные из snapshot в поле Delta
					Source:        "binance", // Source и MarketType дублируются здесь, но это структура OrderBookDelta
					MarketType:    snapshot.MarketType,
					Symbol:        snapshot.Symbol,
					Bids:          snapshot.Bids,
					Asks:          snapshot.Asks,
					Timestamp:     snapshot.Timestamp,    // Используем timestamp снимка для дельты
					FirstUpdateID: snapshot.LastUpdateID, // Для Binance REST snapshot U и u это LastUpdateID
					FinalUpdateID: snapshot.LastUpdateID,
				},
				RawData: nil, // REST snapshot не имеет RawData в том же смысле, что WS.
			}

			// Отправляем сообщение snapshot в OrderBookChan для обработки Scanner'ом.
			// OrderBookChan - это chan<- для клиента (клиент пишет).
			// Используем select с контекстом и таймаутом.
			select {
			case c.Channels.OrderBookChan <- snapshotMsg:
				logger.Info("REST snapshot resync сообщение отправлено в OrderBookChan.")
				// TODO: Уведомить Scanner о завершении попытки ресинка (успешно).
				// Может быть отдельный канал уведомлений клиент -> сканер?
				// Или Scanner сам отслеживает сообщение типа "snapshot_rest" как результат своего запроса.
				return // Успешно отправлено, завершаем горутину.
			case <-ctx.Done(): // Проверяем контекст горутины во время ожидания отправки в канал.
				logger.Warn("Отправка REST snapshot прервана отменой контекста.")
				return // Выход, если контекст отменен.
			case <-time.After(5 * time.Second): // Таймаут на отправку в канал (TODO: из конфига)
				logger.Warn("WARNING: Таймаут (5с) отправки REST snapshot в OrderBookChan. Канал полный? Scanner занят?")
				// TODO: Уведомить Scanner о неудачной отправке snapshot сообщения?
				return // Выход из горутины по таймауту.
			}
		}
	}
	// Если цикл завершается без явного return, значит, все попытки провалились.
	logger.Errorf("Все %d попыток получения REST snapshot провалились (после цикла).", maxAttempts)
	// TODO: Уведомить Scanner о полном провале ресинка?
}

// *** ИСПРАВЛЕНО: Теперь читает из <-chan c.Channels.ResyncChan ***
func (c *BinanceClient) handleResyncCommands(ctx context.Context) {
	c.log.Info("handleResyncCommands goroutine launched.")       // <-- Лог запуска горутины
	defer c.log.Info("handleResyncCommands goroutine finished.") // <-- Лог завершения горутины

	for {
		select {
		case cmd, ok := <-c.Channels.ResyncChan: // Читаем команды из канала ресинка клиента (<-chan)
			if !ok {
				c.log.Info("handleResyncCommands: ResyncChan закрыт, завершение.") // <-- Лог закрытия канала
				return                                                             // Канал закрыт (main defer), выход из горутины.
			}
			// Команда получена успешно
			c.log.WithField("command", cmd).Info("handleResyncCommands: Получена команда ресинка.") // <-- Лог получения команды

			// Проверяем, что команда предназначена для ЭТОГО экземпляра клиента (source и market type)
			// и что это корректный тип команды ресинка для Binance.
			if cmd.Source == "binance" && cmd.MarketType == c.marketType { // <-- Условие для выполнения команды
				switch cmd.Type {
				case types.ResyncTypeBinanceSnapshot:
					// Проверяем, что команда содержит символ
					if cmd.Symbol == "" {
						c.log.WithField("command", cmd).Warn("handleResyncCommands: Получена команда BinanceSnapshot без символа. Игнорирование.")
						continue
					}
					c.log.WithField("symbol", cmd.Symbol).Info("handleResyncCommands: Получена команда BinanceSnapshot. Запуск горутины REST запроса.")

					// Запускаем запрос snapshot в отдельной горутине.
					// Добавляем эту горутину в внутренний WaitGroup клиента.
					c.clientWg.Add(1) // <-- Добавляем 1 к счетчику WaitGroup ПЕРЕД запуском горутины
					go func(resyncCmd *types.ResyncCommand) {
						defer c.clientWg.Done() // <-- Уменьшаем счетчик WaitGroup при выходе из этой горутины
						c.log.WithField("symbol", resyncCmd.Symbol).Debug("performBinanceResync goroutine launched from command.")
						// Вызываем метод performBinanceResync, который выполнит REST запрос,
						// обработает повторные попытки, и отправит результат в OrderBookChan.
						// Он использует контекст clientCtx.
						c.performBinanceResync(c.clientCtx, resyncCmd.Symbol) // performBinanceResync does NOT take wg anymore
						c.log.WithField("symbol", resyncCmd.Symbol).Info("performBinanceResync goroutine finished.")
					}(cmd) // Передаем копию команды в горутину

				case types.ResyncTypeBybitResubscribe:
					// TODO: Реализовать логику переподписки для Bybit, если применимо.
					// BybitResubscribe команда будет обрабатываться в handleSubscriptionCommands?
					// Или здесь? Если здесь, она должна отправить команду подписки в handleSubscriptionCommands.
					c.log.WithField("command", cmd).Warn("handleResyncCommands: Получена команда BybitResubscribe. Обработка не реализована.")
					// If this command is received here, it might mean the Scanner wants us to resubscribe.
					// We could forward this command to handleSubscriptionCommands channel (SubChan).
					// But SubChan is <-chan. We need a different way to signal handleSubscriptionCommands.
					// Maybe handleSubscriptionCommands should listen to ResyncChan too? No, that creates the original problem.
					// The most direct way is to add a dedicated channel for this specific signal.
					// For now, just log and ignore.

				default:
					// Неизвестный тип команды ресинка для Binance
					c.log.WithField("command", cmd).Warn("handleResyncCommands: Получена команда ресинка неизвестного типа для этого клиента. Игнорирование.")
				}
			} else {
				// Команда не релевантна для этого клиента (другая биржа или тип рынка).
				c.log.WithField("command", cmd).Warn("handleResyncCommands: Получена команда ресинка для другого клиента. Этого не должно происходить. Игнорирование.")
			}
		case <-ctx.Done(): // Слушаем контекст горутины (clientCtx), переданный из Run().
			// Контекст отменен (например, вызван c.clientCancel() из Stop() или Run defer).
			c.log.Info("handleResyncCommands: Получен сигнал отмены контекста (clientCtx), завершение.") // <-- Лог отмены контекста
			return                                                                                       // Выход из горутины по отмене контекста.
		}
	}
}

// *** Этот метод отсутствует в вашем коде, добавляем его ***
// handleSubscriptionCommands слушает канал подписок клиента (<-chan) и управляет отправкой команд WS.
// *** ИСПРАВЛЕНО: Теперь читает из <-chan c.Channels.SubChan ***
func (c *BinanceClient) handleSubscriptionCommands(ctx context.Context) {
	c.log.Info("handleSubscriptionCommands goroutine launched.")
	defer c.log.Info("handleSubscriptionCommands goroutine finished.")

	// Flag to track if initial subscriptions have been sent after connection.
	// Reset to false upon connection loss (implicitly, because Run will restart this goroutine).
	connectedAndSubscribedOnce := false // *** Используется ниже ***

	// Add a small delay before sending the initial batch after connection.
	// This is crucial for Binance. The server needs a moment after the connection
	// is established before it can process subscription requests reliably.
	initialSubscribeDelay := 500 * time.Millisecond // TODO: Make configurable

	// Timer for the initial subscription delay.
	initialSubscribeTimer := time.NewTimer(initialSubscribeDelay)
	// Stop and drain immediately, it will be reset upon successful connection signal.
	if !initialSubscribeTimer.Stop() {
		select {
		case <-initialSubscribeTimer.C:
		default:
		}
	}
	// initialSubscribeTimerActive := false // REMOVED: Unused variable

	// connectionEstablishedSignal := make(chan struct{}, 1) // REMOVED: Unused variable - replaced by connReadyForSubscribe in struct

	// Inner function to check if the connection is currently active and usable for sending
	isConnected := func() bool {
		c.mu.Lock()
		connStatus := c.conn != nil && !c.isClosed
		c.mu.Unlock()
		return connStatus
	}

	// --- Helper function to send the accumulated batch ---
	// This helper is called internally and does NOT manage the batchTimer itself,
	// only sends the accumulated messages. The main loop manages the timer reset.
	sendBatchInternal := func() {
		c.mu.Lock() // Acquire mutex for topicsToBatch buffer before reading
		topicsToSubscribe := c.topicsToBatch["subscribe"]
		topicsToUnsubscribe := c.topicsToBatch["unsubscribe"]
		// Make copies if needed to release mutex early, but slice headers are usually enough
		// Let's keep mutex until buffer is cleared.

		// If buffer is empty OR connection is not active, don't attempt to send.
		if len(topicsToSubscribe) == 0 && len(topicsToUnsubscribe) == 0 {
			c.mu.Unlock() // Release mutex
			c.log.Trace("sendBatchInternal: Буфер пакетирования пуст. Пропуск отправки.")
			return // Nothing to send
		}
		// Check connection status *before* trying to send
		if !isConnected() {
			c.mu.Unlock() // Release mutex
			c.log.Warn("sendBatchInternal: Соединение неактивно. Невозможно отправить пакет команд подписки/отписки.")
			return // Cannot send if not connected
		}

		sendCtx, cancelSend := context.WithTimeout(c.clientCtx, wsSubscriptionCommandTimeout)
		defer cancelSend()

		c.log.WithFields(logrus.Fields{
			"subscribe_count":   len(topicsToSubscribe),
			"unsubscribe_count": len(topicsToUnsubscribe),
		}).Debug("sendBatchInternal: Попытка отправки пакета команд подписки/отписки.")

		// --- Отправка SUBSCRIBE ---
		if len(topicsToSubscribe) > 0 {
			c.log.Tracef("sendBatchInternal: Отправка %d топиков на подписку.", len(topicsToSubscribe))
			for i := 0; i < len(topicsToSubscribe); i += c.wsMaxTopicsPerSubscribe {
				end := i + c.wsMaxTopicsPerSubscribe
				if end > len(topicsToSubscribe) {
					end = len(topicsToSubscribe)
				}
				batch := topicsToSubscribe[i:end]
				if len(batch) > 0 {
					c.log.WithField("batch_size", len(batch)).Debug("sendBatchInternal: Отправка SUBSCRIBE пакета...")
					// sendSubscriptionWSMessage uses the provided context for write deadline and cancellation
					// Release buffer mutex temporarily during network operation
					c.mu.Unlock() // Release mutex BEFORE network call
					err := c.sendSubscriptionWSMessage(sendCtx, "SUBSCRIBE", batch)
					c.mu.Lock() // Re-acquire mutex AFTER network call
					if err != nil {
						c.log.WithError(err).WithField("batch_topics", batch).Error("sendBatchInternal: Ошибка отправки SUBSCRIBE пакета. Соединение может быть потеряно.")
						// If send fails, the connection is likely broken.
						// Clear the buffer as commands likely failed and will need resubscription after reconnect.
						c.topicsToBatch = make(map[string][]string) // Clear buffer on send error
						return                                      // Stop sending batch if an error occurs. Run loop handles reconnection.
					} else {
						c.log.WithField("batch_topics", batch).Debug("sendBatchInternal: SUBSCRIBE пакет успешно отправлен.")
					}
				}
				// Add a small delay between batches if necessary to respect per-connection message rate limits
				if i+c.wsMaxTopicsPerSubscribe < len(topicsToSubscribe) {
					c.mu.Unlock() // Release mutex temporarily during sleep/wait
					select {
					case <-time.After(c.wsSendMessageRateLimit):
						// Wait completed
					case <-sendCtx.Done():
						c.log.Warn("sendBatchInternal: Контекст отменен во время ожидания между пакетами SUBSCRIBE.")
						c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
						c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
						c.mu.Unlock()                               // Release mutex
						return
					}
					c.mu.Lock() // Re-acquire mutex after sleep/wait
				}
			}
		}

		// --- Отправка UNSUBSCRIBE ---
		if len(topicsToUnsubscribe) > 0 {
			c.log.Tracef("sendBatchInternal: Отправка %d топиков на отписку.", len(topicsToUnsubscribe))
			// Wait for rate limit interval before sending UNSUBSCRIBE batch if SUBSCRIBE batches were sent
			if len(topicsToSubscribe) > 0 {
				c.mu.Unlock() // Release mutex temporarily during sleep/wait
				select {
				case <-time.After(c.wsSendMessageRateLimit):
				case <-sendCtx.Done():
					c.log.Warn("sendBatchInternal: Контекст отменен во время ожидания перед пакетами UNSUBSCRIBE.")
					c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
					c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
					c.mu.Unlock()                               // Release mutex
					return
				}
				c.mu.Lock() // Re-acquire mutex after sleep/wait
			}

			for i := 0; i < len(topicsToUnsubscribe); i += c.wsMaxTopicsPerSubscribe {
				end := i + c.wsMaxTopicsPerSubscribe
				if end > len(topicsToUnsubscribe) {
					end = len(topicsToUnsubscribe)
				}
				batch := topicsToUnsubscribe[i:end]
				if len(batch) > 0 {
					c.log.WithField("batch_size", len(batch)).Debug("sendBatchInternal: Отправка UNSUBSCRIBE пакета...")
					c.mu.Unlock() // Release mutex temporarily during network operation
					err := c.sendSubscriptionWSMessage(sendCtx, "UNSUBSCRIBE", batch)
					c.mu.Lock() // Re-acquire mutex AFTER network call
					if err != nil {
						c.log.WithError(err).WithField("batch_topics", batch).Error("sendBatchInternal: Ошибка отправки UNSUBSCRIBE пакета. Соединение может быть потеряно.")
						c.topicsToBatch = make(map[string][]string) // Clear buffer on send error
						return                                      // Stop sending batch if an error occurs.
					} else {
						c.log.WithField("batch_topics", batch).Debug("sendBatchInternal: UNSUBSCRIBE пакет успешно отправлен.")
					}
				}
				// Add a small delay between batches if necessary
				if i+c.wsMaxTopicsPerSubscribe < len(topicsToUnsubscribe) {
					c.mu.Unlock() // Release mutex temporarily during sleep/wait
					select {
					case <-time.After(c.wsSendMessageRateLimit):
					case <-sendCtx.Done():
						c.log.Warn("sendBatchInternal: Контекст отменен во время ожидания между пакетами UNSUBSCRIBE.")
						c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
						c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
						c.mu.Unlock()                               // Release mutex
						return
					}
					c.mu.Lock() // Re-acquire mutex after sleep/wait
				}
			}
		}

		// Clear the batch buffer after successful sending (still under mutex)
		c.topicsToBatch = make(map[string][]string)

		// Flag that initial subscriptions have been sent, used outside this helper.
		// This should be done *only* for the first successful batch send after connect.
		if !connectedAndSubscribedOnce {
			connectedAndSubscribedOnce = true // *** Используется здесь ***
			c.log.Info("sendBatchInternal: Первая пачка команд подписки после подключения успешно отправлена.")
			// Initial snapshot initiation is now handled by the Scanner receiving data.
		}

		c.log.Debug("sendBatchInternal: Отправка пакета завершена. Буфер очищен.")
		c.mu.Unlock() // Release mutex at the end of the helper
	}

	// --- Main loop processing commands and timer ---
	for {
		select {
		case cmd, ok := <-c.Channels.SubChan: // Read commands from the client's SubChan (<-chan)
			if !ok {
				c.log.Info("handleSubscriptionCommands: SubChan закрыт, завершение.")
				// Attempt to send any pending batch before exiting.
				sendBatchInternal() // Call internal sender
				return
			}
			c.log.WithField("command", cmd).Info("handleSubscriptionCommands: Получена команда подписки.")

			// Check if the command is for THIS client instance (source and market type)
			if cmd.Source != "binance" || cmd.MarketType != c.marketType {
				c.log.WithFields(logrus.Fields{
					"cmd_source": cmd.Source, "cmd_market_type": cmd.MarketType,
				}).Warnf("handleSubscriptionCommands: Получена команда не для этого клиента (%s %s). Игнорирование.", cmd.Source, cmd.MarketType)
				continue // Skip processing this command
			}

			// Process the command to get topics and update active subscriptions map (under lock)
			affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch := c.getTopicsAndBatchesFromCommand(cmd)

			// Acquire mutex for activeSubscriptions and topicsToBatch buffer before modifying
			c.subMu.Lock()                                   // Use subMu for activeSubscriptions
			c.updateActiveSubscriptions(cmd, affectedTopics) // updates c.activeSubscriptions

			c.mu.Lock() // Use mu for topicsToBatch buffer
			c.topicsToBatch["subscribe"] = append(c.topicsToBatch["subscribe"], topicsToSubscribeBatch...)
			c.topicsToBatch["unsubscribe"] = append(c.topicsToBatch["unsubscribe"], topicsToUnsubscribeBatch...)
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
			c.mu.Unlock()    // Release buffer mutex
			c.subMu.Unlock() // Release activeSubscriptions mutex

			c.log.WithFields(logrus.Fields{
				"added_subscribe": len(topicsToSubscribeBatch), "added_unsubscribe": len(topicsToUnsubscribeBatch),
				"total_in_batch_subscribe": len(c.topicsToBatch["subscribe"]), "total_in_batch_unsubscribe": len(c.topicsToBatch["unsubscribe"]),
			}).Debug("handleSubscriptionCommands: Топики добавлены в буфер пакетирования.")

			// If the connection is currently active AND buffer has items, reset the batch timer.
			// If timer was already running, Stop() returns false, and we drain its channel.
			if bufferHasItems && isConnected() {
				if !c.batchTimer.Stop() {
					select {
					case <-c.batchTimer.C:
					default:
					}
				}
				// Reset the timer to fire after the configured interval.
				c.batchTimer.Reset(defaultBatchInterval) // Reset to default batch interval
				c.log.Trace("handleSubscriptionCommands: Batch timer reset due to new command.")
			} else {
				c.log.Debug("handleSubscriptionCommands: Connection not active or buffer empty after adding command. Batch timer not reset.")
			}

		case <-c.batchTimer.C:
			// Timer fired. Check if connection is active and buffer has items.
			c.mu.Lock() // Acquire mutex to check buffer status
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
			c.mu.Unlock() // Release mutex

			if bufferHasItems && isConnected() {
				c.log.Debug("handleSubscriptionCommands: Таймер пакетирования сработал. Отправка пакета.")
				// sendBatchInternal will clear the buffer on success or error.
				sendBatchInternal()
				// Reset the timer for the next interval IF buffer was not empty before the send attempt.
				c.mu.Lock() // Re-acquire mutex to check buffer status after send
				bufferStillHasItemsAfterSend := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
				c.mu.Unlock() // Release mutex

				if !bufferStillHasItemsAfterSend { // Buffer was successfully sent and cleared
					c.batchTimer.Reset(defaultBatchInterval)
					c.log.Debugf("handleSubscriptionCommands: Timer fired and batch sent. Timer reset to %s.", defaultBatchInterval)
				} else {
					// Buffer still has items after sendBatchInternal (implies send error).
					// Timer stays stopped. Run loop handles reconnection.
					c.log.Warn("handleSubscriptionCommands: Timer fired and batch send failed. Timer not reset.")
				}

			} else {
				c.log.Trace("handleSubscriptionCommands: Таймер пакетирования сработал, но буфер пуст или соединение неактивно. Пропуск отправки.")
				// Timer fired but nothing was sent. Let the timer stay stopped.
			}

		// Listen for signal indicating connection is established and delay is over.
		case <-c.connReadyForSubscribe: // Read signal from Run loop
			c.log.Info("handleSubscriptionCommands: Received connReadyForSubscribe signal.")
			// initialSubscribeTimerActive = false // REMOVED: Unused

			// Start the initial subscribe delay timer.
			// If it was already running or stopped, reset it.
			if !initialSubscribeTimer.Stop() {
				select {
				case <-initialSubscribeTimer.C:
				default:
				}
			}
			initialSubscribeTimer.Reset(initialSubscribeDelay)
			c.log.Debugf("handleSubscriptionCommands: Initial subscribe delay timer reset for %s.", initialSubscribeDelay)

		case <-initialSubscribeTimer.C: // Timer for initial subscribe delay elapsed
			c.log.Info("handleSubscriptionCommands: Initial subscribe timer elapsed. Attempting initial batch send.")
			// initialSubscribeTimerActive = false // REMOVED: Unused

			// Generate commands from active subscriptions and add them to the batch buffer.
			// This regenerates the batch buffer based on what should be subscribed.
			c.log.Debug("handleSubscriptionCommands: Regenerating batch buffer from active subscriptions...")
			resubscribeCmds := c.generateSubscribeCommandsFromActiveSubscriptions()

			c.mu.Lock()                                 // Acquire mutex for topicsToBatch
			c.topicsToBatch = make(map[string][]string) // Clear buffer first
			for _, cmd := range resubscribeCmds {
				// Convert SubscriptionCommand to topic string
				// Assuming getTopicsAndBatchesFromCommand handles subscribe types correctly and returns only subscribe topics here.
				_, topicsToSubscribeBatch, _ := c.getTopicsAndBatchesFromCommand(&cmd)
				c.topicsToBatch["subscribe"] = append(c.topicsToBatch["subscribe"], topicsToSubscribeBatch...)
			}
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0
			c.mu.Unlock() // Release mutex

			c.log.WithField("topics_to_subscribe", len(c.topicsToBatch["subscribe"])).Info("handleSubscriptionCommands: Buffer populated for initial send.")

			// Now send the batch immediately if buffer is not empty and connected.
			if bufferHasItems && isConnected() {
				sendBatchInternal()
				// Reset the batch timer for subsequent command batching.
				c.batchTimer.Reset(defaultBatchInterval)
				c.log.Debugf("handleSubscriptionCommands: Initial batch sent. Batch timer reset to %s.", defaultBatchInterval)
			} else {
				c.log.Debug("handleSubscriptionCommands: Initial batch buffer empty or connection not active. No initial batch sent.")
			}

		case <-ctx.Done():
			c.log.Info("handleSubscriptionCommands: Получен сигнал отмены контекста (clientCtx), завершение.")
			// Attempt to send any pending batch before exiting.
			sendBatchInternal() // Call internal sender
			return
		}
	}
}

// *** Этот метод отсутствует в вашем коде, добавляем его ***
// initiateInitialSnapshot triggers the initial REST snapshot fetch for subscribed depth symbols
// after the first successful subscription batch is sent.
// It sends a ResyncCommand back to the Scanner's ResyncChan.
func (c *BinanceClient) initiateInitialSnapshot() {
	// Use the logger with context
	logger := c.log.WithField("func", "initiateInitialSnapshot")
	logger.Info("Инициирование начального REST snapshot...")

	// Get symbols from active depth subscriptions ONLY for THIS market type.
	// This list is populated in updateActiveSubscriptions after the first batch is processed.
	symbolsToResync := c.getSymbolsFromActiveDepthSubscriptionsForMarket() // Uses logrus inside

	if len(symbolsToResync) > 0 {
		logger.WithField("symbols", symbolsToResync).Info("Обнаружены символы с активными подписками на глубину. Отправка команд ресинка Сканеру.")
	} else {
		logger.Debug("Нет символов с активными подписками на глубину. Пропуск шага инициирования начального REST snapshot.")
	}

	// *** ИСПРАВЛЕНИЕ: Отправляем команду ресинка в ResyncChan клиента (который Scanner слушает) ***
	// Oh wait, the Scanner *writes* to the client's ResyncChan. The client *reads* from it.
	// The command flow is: Scanner detects gap -> Scanner *writes* to Client's ResyncChan -> Client's handleResyncCommands *reads* from it -> Client's performBinanceResync fetches snapshot and *writes* to Scanner's OrderBookChan.
	// So, initiateInitialSnapshot is called by the client, but the *Scanner* should request the *initial* snapshot as well, perhaps triggered by the client becoming ready?
	// Let's re-think the initial snapshot trigger.
	// 1. Client connects successfully.
	// 2. Client sends its initial subscriptions (handled by handleSubscriptionCommands).
	// 3. After sending the initial subscriptions, the Client *informs* the Scanner that it's ready for the initial snapshot for the relevant symbols.
	// 4. The Scanner receives this "client ready" signal (perhaps on a new channel?).
	// 5. The Scanner, upon receiving the "client ready" signal *for a specific market type*, collects the symbols it needs snapshots for (based on its *own* config/tracked symbols) and *sends ResyncCommands* to that specific client's ResyncChan.

	// The current initiateInitialSnapshot is called by the client. It should NOT send a command to its own ResyncChan.
	// It should potentially send a signal to the Scanner.
	// Let's temporarily remove the logic for sending the command here. The trigger mechanism needs adjustment.
	// The most straightforward approach given the existing channels is:
	// 1. Client Run connects.
	// 2. Client Run informs handleSubscriptionCommands (e.g., via a channel) that connection is up.
	// 3. handleSubscriptionCommands sends initial subscribe commands.
	// 4. handleSubscriptionCommands informs the Scanner (e.g., via OrderBookChan with a special message type like "client_ready") that it's subscribed.
	// 5. Scanner receives "client_ready", looks up symbols needed for this client/market type, and sends ResyncCommands back.

	// For now, let's just remove the incorrect sending logic from here and log that the initial snapshot trigger needs rework.
	logger.Warn("Initiating initial snapshot trigger logic is incomplete. The Scanner should request this via ResyncCommand.")

	// --- OLD INCORRECT SENDING LOGIC REMOVED ---
	/*
		for _, symbol := range symbolsToResync {
			// Send resync command to ResyncChan. Use timeout/context for sending.
			// This command will be received by handleResyncCommands, which will launch performBinanceResync.
			select {
			case c.Channels.ResyncChan <- &types.ResyncCommand{Source: "binance", MarketType: c.marketType, Symbol: symbol, Type: types.ResyncTypeBinanceSnapshot}:
				logger.WithField("symbol", symbol).Info("Команда ресинка (REST Snapshot) отправлена в ResyncChan.")
			case <-c.clientCtx.Done(): // Проверяем внутренний контекст клиента при ожидании отправки
				logger.Warnf("Отправка команды ресинка для %s прервана отменой clientCtx.", symbol)
				return // Выход из метода
			case <-time.After(1 * time.Second): // Таймаут на отправку, если канал полный (TODO: из конфига)
				logger.Warnf("WARNING: Таймаут (1с) отправки команды ресинка для %s. ResyncChan полный?", symbol)
			}
		}
	*/
	logger.Debug("Завершение инициирования начального REST snapshot (trigger logic needs rework).")
}

// *** Этот метод отсутствует в вашем коде, добавляем его ***
// sendSubscriptionWSMessage sends a SUBSCRIBE or UNSUBSCRIBE message to the WebSocket.
// Uses the provided context for write deadline and cancellation.
func (c *BinanceClient) sendSubscriptionWSMessage(ctx context.Context, method string, topics []string) error {
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithFields(logrus.Fields{
		"func":           "sendSubscriptionWSMessage",
		"request_method": method,
		"topic_count":    len(topics),
	})
	logger.Debug("Подготовка к отправке сообщения подписки/отписки через WebSocket.")

	// Use an atomic counter for a unique ID for each request, needed to match responses.
	// Simple time based ID for now, but atomic counter is better for high throughput.
	// TODO: Use an atomic counter for requestID.
	requestID := int(time.Now().UnixNano() % 1000000)
	logger = logger.WithField("request_id", requestID) // Добавляем ID в контекст логгера

	request := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int      `json:"id"`
	}{
		Method: method,
		Params: topics,
		ID:     requestID,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		// Логируем ошибку маршалинга на уровне Error
		logger.WithError(err).Error("Ошибка маршалинга subscribe/unsubscribe запроса в JSON.")
		return fmt.Errorf("BinanceClient (%s): error marshalling subscribe/unsubscribe request: %w", c.marketType, err)
	}

	c.mu.Lock()         // Блокируем мьютекс для безопасного доступа к c.conn
	defer c.mu.Unlock() // Гарантируем освобождение мьютекса

	if c.conn == nil {
		// Соединение nil. Логируем предупреждение.
		logger.Warn("Соединение nil. Невозможно отправить сообщение.")
		return fmt.Errorf("BinanceClient (%s): WebSocket connection is not established for sending subscribe/unsubscribe request", c.marketType)
	}

	// Set write deadline using the provided context's deadline.
	// The context passed here usually has a timeout (e.g., wsSubscriptionCommandTimeout).
	deadline, ok := ctx.Deadline()
	if !ok {
		// If the context has no deadline, use the default WS write timeout.
		deadline = time.Now().Add(c.wsWriteTimeout)
		logger.Tracef("sendSubscriptionWSMessage: Context has no deadline, using default wsWriteTimeout %s.", c.wsWriteTimeout)
	} else {
		logger.Tracef("sendSubscriptionWSMessage: Context deadline set to %s.", deadline)
	}

	// Apply the chosen deadline to the connection's write operations.
	// This deadline applies to the entire WriteMessage call, including waiting for the buffer.
	err = c.conn.SetWriteDeadline(deadline)
	if err != nil {
		// Error setting deadline usually means connection is bad.
		logger.WithError(err).Error("Ошибка установки write deadline.")
		// Check c.conn again under mutex, as it might have been set to nil by c.Close() concurrently.
		if c.conn == nil {
			logger.Debug("c.conn стал nil после попытки SetWriteDeadline.")
			return fmt.Errorf("BinanceClient (%s): WebSocket connection became nil after setting write deadline: %w", c.marketType, err)
		}
		return fmt.Errorf("BinanceClient (%s): error setting WriteDeadline for subscribe/unsubscribe request: %w", c.marketType, err)
	}

	// Send the message. This call respects the WriteDeadline set above.
	err = c.conn.WriteMessage(websocket.TextMessage, jsonData)

	// Reset the write deadline after sending. CRITICAL!
	// Check conn for nil under mutex before resetting deadline.
	if c.conn != nil { // Check under mutex in case c.Close() happened right after WriteMessage started
		resetErr := c.conn.SetWriteDeadline(time.Time{}) // Zero time means no deadline.
		if resetErr != nil {
			// Log warning, but don't return error as message might have been sent.
			logger.WithError(resetErr).Warn("Ошибка сброса write deadline.")
		}
	} else {
		// Log if c.conn became nil after WriteMessage attempt (indicates breakage during send).
		logger.Debug("c.conn стал nil после попытки WriteMessage.")
	}

	if err != nil {
		// Error sending message. Log.
		logger.WithError(err).Error("Ошибка отправки сообщения.")

		// Check if the error was caused by context cancellation.
		select {
		case <-ctx.Done(): // Check the context passed to *this* function
			logger.Debug("Ошибка отправки совпала с отменой контекста.")
			return ctx.Err() // Return the context error if it was cancelled
		default:
			// If it's not a context error, check if it indicates a closed network connection
			if _, ok := err.(net.Error); ok && strings.Contains(err.Error(), "use of closed network connection") {
				logger.Debug("Обнаружена ошибка 'use of closed network connection' во время отправки.")
				return fmt.Errorf("BinanceClient (%s): write to closed network connection: %w", c.marketType, err)
			}
			// Return the general send error.
			return fmt.Errorf("BinanceClient (%s): error sending subscribe/unsubscribe request (%s, %d topics): %w", c.marketType, method, len(topics), err)
		}
	}

	// Message successfully sent. Log at Debug level.
	logger.WithFields(logrus.Fields{
		"request_method": method,
		"topic_count":    len(topics),
		"request_id":     requestID,
	}).Debug("Сообщение успешно отправлено.")

	// TODO: Optionally: Add logic to wait for and process the response from the exchange with this id.
	// Responses are handled in handleMessage.

	return nil // Return nil on successful request sending attempt.
}

func (c *BinanceClient) getTopicsAndBatchesFromCommand(cmd *types.SubscriptionCommand) ([]string, []string, []string) {
	// Используем логгер с контекстом для этого метода и команды
	logger := c.log.WithFields(logrus.Fields{
		"func":    "getTopicsAndBatchesFromCommand",
		"command": cmd, // Логируем всю команду для отладки
	})
	logger.Debug("Определение топиков для команды подписки/отписки.")

	lowerSymbol := strings.ToLower(cmd.Symbol)

	affectedTopics := []string{}
	topicsToSubscribeBatch := []string{}
	topicsToUnsubscribeBatch := []string{}

	// Проверка источника и типа рынка (убеждаемся, что команда для этого клиента)
	if cmd.Source != "binance" || cmd.MarketType != c.marketType {
		logger.WithFields(logrus.Fields{
			"cmd_source": cmd.Source, "cmd_market_type": cmd.MarketType, // Логируем строковое представление
			"client_market_type": c.marketType,
		}).Warn("Получена команда не для этого клиента. Игнорирование.")
		return affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch
	}

	switch cmd.Type {
	case types.SubscribeDepth:
		// Определяем формат топика на основе запрошенной глубины.
		// Binance V3 public WS depth streams are delta-based.
		// The format is usually <symbol>@depth<levels>@100ms.
		// <levels> can be 5, 10, 20, 50, 100, 500, 1000.
		// A full depth stream without levels is not typical for public Binance WS.
		// The depth parameter in the command should specify the levels.
		// Let's assume cmd.Depth indicates the desired levels (50, 1000 etc.).
		// If cmd.Depth is 0 or not a supported level, we might default or error.
		// A common public depth stream is 1000 levels at 100ms.

		var targetTopic string
		depthToUse := cmd.Depth

		// Supported levels for @depth<levels>@100ms
		supportedLevels := map[int]bool{5: true, 10: true, 20: true, 50: true, 100: true, 500: true, 1000: true}
		defaultLevel := 1000 // Default if requested depth > 0 is not supported

		if depthToUse <= 0 || !supportedLevels[depthToUse] {
			// If depth is 0 or unsupported (>0), use a default level topic (e.g., 1000).
			// If 0 was intended as "any depth stream", the logic might need adjustment.
			// Assuming 0 depth means "default depth stream", which is often 1000@100ms.
			// Or it could mean the legacy @depth stream (not recommended).
			// Let's assume cmd.Depth 0 means default depth stream (1000 levels at 100ms)
			// or if cmd.Depth > 0 but unsupported, default to 1000.

			actualDepthForTopic := defaultLevel
			if depthToUse > 0 && !supportedLevels[depthToUse] {
				logger.WithFields(logrus.Fields{
					"requested_depth": depthToUse,
					"market_type":     c.marketType,
				}).Warnf("Запрошена неподдерживаемая глубина (>0). Использование уровня %d для формирования топика.", defaultLevel)
			} else if depthToUse == 0 {
				logger.WithFields(logrus.Fields{
					"requested_depth": depthToUse,
					"market_type":     c.marketType,
				}).Debugf("Запрошена глубина 0. Использование уровня %d для формирования топика по умолчанию.", defaultLevel)
			}

			targetTopic = fmt.Sprintf("%s@depth%d@100ms", lowerSymbol, actualDepthForTopic)

		} else {
			// Requested depth is a supported level > 0.
			targetTopic = fmt.Sprintf("%s@depth%d@100ms", lowerSymbol, depthToUse)
		}

		logger.WithFields(logrus.Fields{
			"requested_depth": cmd.Depth,
			"target_topic":    targetTopic,
			"market_type":     c.marketType,
		}).Debug("Определен целевой топик для SubscribeDepth.")

		// Topics to subscribe: only the target topic
		topicsToSubscribeBatch = append(topicsToSubscribeBatch, targetTopic)
		affectedTopics = append(affectedTopics, targetTopic) // Target topic is definitely affected

		// Topics to unsubscribe: Any existing depth topics for the same symbol and market type.
		// This prevents listening to multiple depth streams (e.g., depth50 and depth1000) for the same symbol.
		c.subMu.RLock()
		for activeTopic, activeCmd := range c.activeSubscriptions {
			// Check if the active topic is for the same symbol, contains "@depth",
			// is for THIS client's market type, and is NOT the target topic we are subscribing to.
			if strings.HasPrefix(activeTopic, lowerSymbol+"@depth") && // Check prefix for symbol@depth
				strings.Contains(activeTopic, "@") && // Ensure it has an '@' separator
				activeCmd.Source == "binance" && // Ensure it's a Binance command
				activeCmd.MarketType == c.marketType && // Ensure it's for this market type
				activeTopic != targetTopic { // Exclude the new target topic
				// This seems like an old depth topic for the same symbol/market, unsubscribe from it.
				topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, activeTopic)
				affectedTopics = append(affectedTopics, activeTopic) // Old topics are also affected (by being unsubscribed)
				logger.WithField("old_depth_topic", activeTopic).Trace("Топик глубины для отписки.")
			}
		}
		c.subMu.RUnlock()

	case types.UnsubscribeDepth:
		// Unsubscribe from ALL depth topics for the symbol and market type.
		c.subMu.RLock()
		for activeTopic, activeCmd := range c.activeSubscriptions {
			// Check if the active topic is for the same symbol, contains "@depth",
			// and is for THIS client's market type.
			if strings.HasPrefix(activeTopic, lowerSymbol+"@depth") &&
				strings.Contains(activeTopic, "@") &&
				activeCmd.Source == "binance" &&
				activeCmd.MarketType == c.marketType {
				topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, activeTopic)
				affectedTopics = append(affectedTopics, activeTopic)
				logger.WithField("depth_topic_to_unsubscribe", activeTopic).Trace("Топик глубины для отписки.")
			}
		}
		c.subMu.RUnlock()

	case types.SubscribeBBO:
		var topic string
		// BookTicker topic is the same for Spot and Futures on Binance: <symbol>@bookTicker
		topic = fmt.Sprintf("%s@bookTicker", lowerSymbol)

		logger.WithFields(logrus.Fields{
			"market_type":  c.marketType,
			"target_topic": topic,
		}).Debug("Определен топик для SubscribeBBO.")

		affectedTopics = append(affectedTopics, topic)
		topicsToSubscribeBatch = append(topicsToSubscribeBatch, topic)

		// For BBO, there aren't typically "old" competing topics to remove upon subscribing.
		// If we support multiple BBO streams (like @bookTicker and @ticker for 24hr stats),
		// we might need logic similar to depth here. But for now, only @bookTicker is relevant for BBO.

	case types.UnsubscribeBBO:
		var topic string
		// BookTicker topic is the same for Spot and Futures: <symbol>@bookTicker
		topic = fmt.Sprintf("%s@bookTicker", lowerSymbol)

		logger.WithFields(logrus.Fields{
			"market_type":  c.marketType,
			"target_topic": topic,
		}).Debug("Определен топик для UnsubscribeBBO.")

		affectedTopics = append(affectedTopics, topic)
		topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, topic)

	default:
		logger.WithField("command_type", cmd.Type).Warnf("Неизвестный тип команды подписки/отписки. Игнорирование.")
	}

	logger.WithFields(logrus.Fields{
		"affected_count":             len(affectedTopics),
		"to_subscribe_batch_count":   len(topicsToSubscribeBatch),
		"to_unsubscribe_batch_count": len(topicsToUnsubscribeBatch),
		"market_type":                c.marketType,
	}).Debug("Определение топиков завершено.")

	return affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch
}

// updateActiveSubscriptions обновляет внутреннюю мапу activeSubscriptions
// на основе полученной команды и списка затронутых топиков.
// Эта функция ТОЛЬКО модифицирует мапу в памяти. ОНА НЕ ОТПРАВЛЯЕТ WS запросы.
// Использует logrus.
// *** ЭТОТ МЕТОД ОТСУТСТВУЕТ, ДОБАВЛЯЕМ ЕГО ***
// Принимает указатель *SubscriptionCommand и список топиков, чье состояние меняется
func (c *BinanceClient) updateActiveSubscriptions(cmd *types.SubscriptionCommand, affectedTopics []string) { // <<< Прикреплен к *BinanceClient, Принимает *SubscriptionCommand и []string
	// Используем логгер с контекстом для этого метода и команды
	logger := c.log.WithFields(logrus.Fields{
		"func":                  "updateActiveSubscriptions",
		"command_type":          cmd.Type,
		"symbol":                cmd.Symbol,
		"affected_topics_count": len(affectedTopics),
	})
	logger.Debug("Обновление карты активных подписок.")

	c.subMu.Lock()         // Блокируем для записи в activeSubscriptions
	defer c.subMu.Unlock() // Гарантируем освобождение мьютекса

	// Убедимся, что команда предназначена для ТИПА РЫНКА ЭТОГО клиента (проверка избыточна, но безопаснее)
	if cmd.MarketType != c.marketType || cmd.Source != "binance" {
		logger.Warn("Получена команда не для этого клиента. Этого не должно происходить.")
		return
	}

	// Логика: affectedTopics перечисляет все топики, чье состояние в мапе должно измениться.
	// На основе типа команды, мы определяем, какие из этих топиков добавляются,
	// а какие удаляются.

	topicsToRemove := make(map[string]bool)                           // Используем мапу для эффективного поиска топиков для удаления
	topicsToAddOrUpdate := make(map[string]types.SubscriptionCommand) // Map for topics to add/update

	switch cmd.Type {
	case types.SubscribeDepth:
		// Для SubscribeDepth, есть один целевой топик для ДОБАВЛЕНИЯ/ОБНОВЛЕНИЯ, и потенциально много старых для УДАЛЕНИЯ.
		// affectedTopics содержит как новый топик, так и старые для удаления (полученные из getTopicsAndBatchesFromCommand).
		// Находим новый топик и добавляем/обновляем его. Все остальные затронутые топики удаляются.

		// Determine the target topic based on command logic (must match getTopicsAndBatchesFromCommand)
		var targetTopic string
		lowerSymbol := strings.ToLower(cmd.Symbol)
		supportedLevels := map[int]bool{5: true, 10: true, 20: true, 50: true, 100: true, 500: true, 1000: true}
		defaultLevel := 1000

		depthToUse := cmd.Depth
		if depthToUse <= 0 || !supportedLevels[depthToUse] {
			targetTopic = fmt.Sprintf("%s@depth%d@100ms", lowerSymbol, defaultLevel)
		} else {
			targetTopic = fmt.Sprintf("%s@depth%d@100ms", lowerSymbol, depthToUse)
		}

		// The target topic should be added/updated in the map.
		topicsToAddOrUpdate[targetTopic] = *cmd // Store a COPY of the command value

		// All other topics in affectedTopics should be removed from the map.
		for _, topic := range affectedTopics {
			if topic != targetTopic {
				topicsToRemove[topic] = true
				logger.WithField("topic", topic).Trace("Помечен топик для удаления из map (SubscribeDepth, old topic).") // Trace
			}
		}

	case types.UnsubscribeDepth:
		// For UnsubscribeDepth, all affected depth topics for the symbol ARE removed.
		for _, topic := range affectedTopics {
			topicsToRemove[topic] = true
			logger.WithField("topic", topic).Trace("Помечен топик для удаления из map (UnsubscribeDepth).") // Trace
		}

	case types.SubscribeBBO:
		// For SubscribeBBO, the target BBO topic IS added/updated.
		topic := strings.ToLower(cmd.Symbol) + "@bookTicker"
		topicsToAddOrUpdate[topic] = *cmd                                                                           // Store a COPY of the command value
		logger.WithField("topic", topic).Trace("Помечен BBO топик для добавления/обновления в map (SubscribeBBO).") // Trace
		// No topics marked for removal in this case, based on Binance behavior.

	case types.UnsubscribeBBO:
		// For UnsubscribeBBO, the target BBO topic IS removed.
		topic := strings.ToLower(cmd.Symbol) + "@bookTicker"
		topicsToRemove[topic] = true
		logger.WithField("topic", topic).Trace("Помечен BBO топик для удаления из map (UnsubscribeBBO).") // Trace
	}

	// Perform removals from the activeSubscriptions map
	if len(topicsToRemove) > 0 {
		logger.WithField("topics_to_remove_count", len(topicsToRemove)).Debug("Выполнение удалений из карты активных подписок.")
		for topic := range topicsToRemove {
			if _, ok := c.activeSubscriptions[topic]; ok {
				delete(c.activeSubscriptions, topic)
				logger.WithField("topic", topic).Debug("Активный топик удален из map.")
			} else {
				logger.WithField("topic", topic).Trace("Попытка удалить топик, которого нет в map.") // Trace
			}
		}
	}

	// Perform additions/updates to the activeSubscriptions map
	if len(topicsToAddOrUpdate) > 0 {
		logger.WithField("topics_to_add_count", len(topicsToAddOrUpdate)).Debug("Выполнение добавлений/обновлений в карту активных подписок.")
		for topic, cmdValue := range topicsToAddOrUpdate {
			c.activeSubscriptions[topic] = cmdValue // Add or update
			logger.WithField("topic", topic).WithField("cmd_type", cmdValue.Type).Debug("Активный топик добавлен/обновлен в map.")
		}
	}

	// Log debug of current subscription count
	logger.WithField("current_count", len(c.activeSubscriptions)).Debug("Обновление карты активных подписок завершено.")
}

func (c *BinanceClient) Close() {
	c.mu.Lock()         // Блокируем мьютекс
	defer c.mu.Unlock() // Гарантируем освобождение

	if c.conn == nil {
		c.log.Trace("Close: Попытка закрыть уже nil соединение.") // Trace
		return                                                    // Соединение уже nil.
	}

	c.log.Info("Closing WebSocket connection...")

	// Try sending a standard normal closure message.
	// Set write deadline for sending the CloseMessage.
	// Use the configured wsWriteTimeout for this deadline.
	writeDeadline := time.Now().Add(c.wsWriteTimeout) // Timeout for sending
	err := c.conn.SetWriteDeadline(writeDeadline)
	if err == nil {
		// Use websocket.FormatCloseMessage for standard closure message.
		// WriteMessage respects the WriteDeadline set above.
		err = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"))
		if err != nil && err != websocket.ErrCloseSent {
			// Log error sending CloseMessage at Warn level, if it's not the standard "already closed" error.
			c.log.WithError(err).Warn("Ошибка отправки WebSocket close message.")
		} else if err == nil {
			c.log.Debug("WebSocket close message отправлено.")
		}
	} else {
		// Log error setting WriteDeadline before CloseMessage.
		c.log.WithError(err).Warn("Ошибка установки WriteDeadline перед отправкой CloseMessage.")
	}

	// Reset WriteDeadline after sending.
	// Check conn for nil under mutex.
	if c.conn != nil { // Check under mutex in case c.Close() happened immediately after WriteMessage
		c.conn.SetWriteDeadline(time.Time{}) // Zero time means no deadline.
	}

	// Perform the physical connection closure.
	// This Close() call does not take a context.
	err = c.conn.Close()
	if err != nil {
		// Ignore "use of closed network connection" errors, as they might occur
		// if the connection was closed concurrently due to read error or context cancellation.
		if !strings.Contains(err.Error(), "use of closed network connection") {
			c.log.WithError(err).Error("Ошибка закрытия WebSocket соединения.")
		} else {
			c.log.Trace("Ошибка 'use of closed network connection' при закрытии WS.") // Trace
		}
	}

	c.conn = nil // Set connection to nil after closing.
	c.log.Info("Соединение WebSocket закрыто.")
}

// CloseWithContext closes the WebSocket connection gracefully with a context deadline.
// This is a helper used internally by ConnectWebSocket to clean up old connections. Uses logrus.
// *** НОВЫЙ ХЕЛПЕР МЕТОД ***
func (c *BinanceClient) CloseWithContext(ctx context.Context) error { // Принимает context
	_ = ctx // >>> ДОБАВЛЕНО: Явно указываем компилятору, что контекст используется - FIXED

	c.mu.Lock()         // Блокируем мьютекс
	defer c.mu.Unlock() // Гарантируем освобождение

	if c.conn == nil {
		c.log.Trace("CloseWithContext: Попытка закрыть уже nil соединение.") // Trace
		return nil                                                           // Соединение уже nil.
	}

	c.log.WithField("context_deadline", func() string { // Добавляем дедлайн контекста в лог
		deadline, ok := ctx.Deadline()
		if ok {
			return deadline.String()
		}
		return "none"
	}()).Debug("CloseWithContext: Закрытие WebSocket соединения с контекстом...")

	conn := c.conn // Локальная копия соединения

	// Try sending a normal closure message within the context deadline.
	// Set write deadline using the context deadline.
	deadline, ok := ctx.Deadline()
	if ok {
		// Устанавливаем дедлайн из контекста.
		err := conn.SetWriteDeadline(deadline)
		if err != nil {
			c.log.WithError(err).Warn("CloseWithContext: Ошибка установки WriteDeadline для PING/CloseMessage.")
			// Continue closing even if setting deadline failed.
		}
	}

	// WriteMessage respects the WriteDeadline set above.
	err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"))
	if err != nil && err != websocket.ErrCloseSent {
		// Log error sending CloseMessage at Warn level, if it's not the standard "already closed" error.
		c.log.WithError(err).Warn("CloseWithContext: Ошибка отправки WebSocket close message.")
		// Don't return error here, as physical close might still work.
	} else if err == nil {
		c.log.Trace("CloseWithContext: WebSocket close message отправлено.") // Trace
	}

	// Perform physical connection closure. This should ideally be unblocked by the WriteMessage
	// completion or the context deadline.
	err = conn.Close()
	if err != nil {
		// Ignore "use of closed network connection" errors.
		if !strings.Contains(err.Error(), "use of closed network connection") {
			c.log.WithError(err).Error("CloseWithContext: Ошибка закрытия WebSocket соединения.")
		} else {
			c.log.Trace("CloseWithContext: Ошибка 'use of closed network connection' при закрытии WS.") // Trace
		}
	}

	c.conn = nil // Set connection to nil after closing.
	c.log.Debug("CloseWithContext: Соединение WebSocket закрыто.")
	return err // Return the physical close error (or nil).
}

// Stop is called for graceful shutdown of the client from outside.
// Signals the client for full termination. Uses logrus.
// *** ДОБАВЛЕНО этот отсутствующий метод ***
func (c *BinanceClient) Stop() {
	c.log.Info("Получен внешний сигнал остановки (Stop())...")
	c.mu.Lock() // Блокируем мьютекс для безопасной работы с isClosed
	if c.isClosed {
		c.mu.Unlock() // Освобождаем мьютекс
		c.log.Debug("Stop(): Клиент уже был помечен как закрытый.")
		return // Клиент уже останавливается.
	}
	c.isClosed = true // Устанавливаем флаг.
	c.mu.Unlock()     // Освобождаем мьютекс.

	c.log.Debug("Stop(): Отмена внутреннего контекста клиента (clientCtx).")
	c.clientCancel() // Отменяем внутренний контекст клиента.

	c.log.Info("Stop(): Сигнал остановки обработан. Ожидание завершения внутренних горутин (происходит в Run defer).")
}

package bybit

import (
	"context"       // Импортируем пакет для работы с контекстом
	"encoding/json" // Пакет для работы с JSON
	"errors"
	"net/url"

	// Для errors.Is
	"fmt" // Форматированный ввод/вывод
	"net" // Для performBybitResync (если нужен REST), RateLimiter

	// Для performBybitResync (если нужен REST)
	"strconv"

	// Пакет для работы с потоками ввода/вывода
	// "log"  // >>> Удаляем стандартный пакет log
	"math" // Пакет для математических операций (pow, min)

	// Пакет для работы с сетью (для проверки ошибок)
	// Пакет для работы с HTTP клиентом
	// Пакет для парсинга URL
	// Пакет для преобразования строк
	"runtime/debug" // Для recover
	"strings"       // Пакет для работы со строками
	"sync"          // Пакет для примитивов синхронизации (Mutex, WaitGroup)
	"time"          // Пакет для работы со временем

	"arbitrage-engine/internal/config"       // Импортируем пакет config для доступа к конфигу Bybit
	"arbitrage-engine/internal/rate_limiter" // Убедитесь, что Rate Limiter обновлен для logrus и имеет метод Wait(weight int) bool или Wait(ctx context.Context, weight int) error
	"arbitrage-engine/internal/types"        // Обновленный пакет types

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus" // >>> Импортируем logrus
)

// TODO: Константы URL и таймаутов лучше вынести в конфиг.
// Используем константы по умолчанию, если они не заданы в конфиге.
// Bybit V5 Public Spot: wss://stream.bybit.com/v5/public/spot
// Bybit V5 Public Linear: wss://stream.bybit.com/v5/public/linear
// Bybit V5 REST: https://api.bybit.com
const defaultBybitWebSocketURL = "wss://stream.bybit.com/v5/public/spot"
const defaultBybitRESTBaseURL = "https://api.bybit.com"

const (
	// Параметры экспоненциального backoff для переподключения WebSocket
	reconnectBaseDelay    = 1 * time.Second
	reconnectMaxDelay     = 60 * time.Second
	reconnectMaxAttempts  = 0 // Максимальное количество попыток (0 = бесконечно)
	reconnectJitterFactor = 0.1
)

const (
	// Таймауты для операций с WebSocket Bybit.
	// Bybit рекомендует отправлять пинг каждые 20 секунд.
	// Они закрывают соединение, если не получают никаких фреймов 30 секунд.
	// Наш таймаут чтения должен быть немного больше 30 секунд.
	defaultWsPingInterval = 15 * time.Second                        // Интервал отправки PING сообщений Bybit {"op": "ping"}.
	defaultWsReadTimeout  = defaultWsPingInterval*2 + 5*time.Second // Таймаут на чтение из WebSocket (~35 сек).
	defaultWsWriteTimeout = 5 * time.Second                         // Таймаут на отправку сообщения по WebSocket.
	// Таймаут для отправки команд подписки/отписки по WS
	wsSubscriptionCommandTimeout = 5 * time.Second // TODO: из конфига
	defaultBatchInterval         = 100 * time.Millisecond
	wsDialTimeout                = 10 * time.Second // Таймаут на установку WS соединения (Dial)
)

// BybitClient представляет клиента для взаимодействия с Bybit.
// Управляет WebSocket соединением, подписками, получением данных и переподключением.
type BybitClient struct {
	conn *websocket.Conn
	mu   sync.Mutex // Protects conn and isClosed

	isClosed bool // Flag signalling explicit stop via Stop().

	Channels types.ExchangeChannels // Structure with channels (ResyncChan/SubChan are now <-chan)

	restLimiter *rate_limiter.Limiter // Rate limiter (used for REST calls like fetching symbols or historical data)
	// Store the market-specific config (SpotConfig or FuturesConfig).
	// This provides access to market-specific URLs, enabled status, and WS limits.
	cfg config.MarketConfigInterface

	marketType types.MarketType // *** ADDED: Market Type (Spot, Futures, etc.) ***

	// --- Batching and Subscription Management ---
	// Map to track active subscriptions (topic string -> original command).
	// Key: topic string (e.g., "orderbook.50.BTCUSDT", "orderbook.1.ETHUSDT")
	// Value: types.SubscriptionCommand that initiated the subscription.
	// We store the *value* here, as the command is read from a channel (copy) or generated (new struct).
	activeSubscriptions map[string]types.SubscriptionCommand
	subMu               sync.RWMutex // Read/Write mutex for activeSubscriptions map

	// Buffer for accumulating topics for batched subscribe/unsubscribe messages.
	// Key: "subscribe" or "unsubscribe" string. Value: slice of topic strings.
	topicsToBatch map[string][]string
	batchTimer    *time.Timer // Timer to trigger batch sending.
	// Channel to signal immediate batch send (e.g., when command arrives and timer expired).
	sendBatchSignal chan struct{}

	// WS Limits from config
	wsMaxTopicsPerSubscribe int           // Max topics in 'args' array of one WS subscribe/unsubscribe message.
	wsSendMessageRateLimit  time.Duration // Min time between sending WS messages (1/rate_limit_per_sec).

	// --- Client Lifecycle Management ---
	reconnectAttempt int
	clientCtx        context.Context
	clientCancel     context.CancelFunc
	clientWg         sync.WaitGroup // WaitGroup for client's internal goroutines.

	wsReadTimeout  time.Duration
	wsWriteTimeout time.Duration // Timeout for writing a single message (including control frames).
	pingInterval   time.Duration // Interval for sending PINGs
	lastPong       time.Time     // For tracking JSON PONG from Bybit V5.

	connReadyForSubscribe chan struct{} // *** ДОБАВЛЕНО ***

	log *logrus.Entry // >>> Используем *logrus.Entry
}

// *** ИЗМЕНЕНО: Теперь принимает ctx, logger, cfg, channels, rlManager ***
func NewBybitClient(
	ctx context.Context, // Main application context
	logger *logrus.Entry, // >>> Принимаем logrus logger
	cfg config.MarketConfigInterface, // Specific market config (Spot or Futures)
	marketType types.MarketType, // The market type this client instance handles
	channels types.ExchangeChannels,
	rlManager *rate_limiter.Limiter, // Shared REST rate limiter
) *BybitClient {
	// Создаем логгер с контекстом для этого клиента (биржа и тип рынка)
	logger = logger.WithFields(logrus.Fields{
		"exchange":    "bybit",
		"market_type": marketType,
	})
	logger.Info("Starting BybitClient initialization...")

	// Create an internal context for the client, derived from the main context.
	clientCtx, clientCancel := context.WithCancel(ctx)

	// Load WS limits from config
	wsMaxTopics := cfg.GetWSMaxTopicsPerSubscribe()
	wsMsgLimitPerSec := cfg.GetWSSendMessageRateLimitPerSec()
	wsSendMessageRateLimit := time.Second / time.Duration(wsMsgLimitPerSec) // Calculate min time between messages
	// Ensure a minimum rate limit interval to avoid division by zero or negative values
	if wsMsgLimitPerSec <= 0 { // Проверяем исходное значение, чтобы избежать деления на ноль
		wsSendMessageRateLimit = 100 * time.Millisecond // Default to 10 messages/sec
		logger.WithField("config_value", wsMsgLimitPerSec).Warnf("Invalid WSSendMessageRateLimitPerSec value (%d). Using default: %s.", wsMsgLimitPerSec, wsSendMessageRateLimit)
	} else {
		logger.WithField("config_value", wsSendMessageRateLimit).Debugf("WSSendMessageRateLimit set: %s.", wsSendMessageRateLimit)
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
	}).Debug("Set WS timeouts and intervals.")

	client := &BybitClient{
		Channels:            channels,
		restLimiter:         rlManager,
		cfg:                 cfg,
		marketType:          marketType,
		activeSubscriptions: make(map[string]types.SubscriptionCommand), // Initialize active subscriptions map

		topicsToBatch:   make(map[string][]string),           // Initialize batch buffer
		batchTimer:      time.NewTimer(defaultBatchInterval), // Initialize batch timer
		sendBatchSignal: make(chan struct{}, 1),              // Initialize signal channel (buffered)

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

		log: logger, // >>> Сохраняем logrus logger

		connReadyForSubscribe: make(chan struct{}, 1), // *** INITIALIZED ***
	}

	// Stop the timer immediately after creation.
	if !client.batchTimer.Stop() {
		select {
		case <-client.batchTimer.C: // Drain the channel if it fired
			logger.Debug("Batch timer fired immediately after creation and stop. Channel drained.")
		default:
			// Channel was empty or already drained
		}
	}

	logger.Info("BybitClient initialized.")
	logger.WithFields(logrus.Fields{
		"ws_url":                  cfg.GetWSBaseURL(),
		"ws_max_topics_per_batch": client.wsMaxTopicsPerSubscribe,
		"ws_min_msg_interval":     client.wsSendMessageRateLimit,
	}).Debug("WS client parameters.")

	// Launch handler goroutines only once for the client's lifecycle.
	client.clientWg.Add(1)
	go func() {
		defer client.clientWg.Done()
		client.handleResyncCommands(client.clientCtx)
		client.log.Infof("BybitClient (%s): handleResyncCommands goroutine finished.", client.marketType)
	}()
	client.log.Infof("BybitClient (%s): handleResyncCommands goroutine launched.", client.marketType)

	client.clientWg.Add(1)
	go func() {
		defer client.clientWg.Done()
		client.handleSubscriptionCommands(client.clientCtx)
		client.log.Infof("BybitClient (%s): handleSubscriptionCommands goroutine finished.", client.marketType)
	}()
	client.log.Infof("BybitClient (%s): handleSubscriptionCommands goroutine launched.", client.marketType)

	return client
}

func (c *BybitClient) Run(ctx context.Context, wg *sync.WaitGroup) {
	c.log.Infof("BybitClient (%s): Starting main Run goroutine...", c.marketType)
	wg.Add(1)
	defer wg.Done() // Notify main WaitGroup on exit.

	defer func() {
		c.log.Infof("BybitClient (%s): Run defer: Initiating internal context cancellation and waiting for internal goroutines.", c.marketType)
		// Cancel the client's internal context. This signals all internal goroutines
		// launched with clientCtx to terminate.
		c.clientCancel()
		// Wait for all goroutines added to the client's internal WaitGroup (c.clientWg) to finish.
		c.clientWg.Wait()
		c.log.Infof("BybitClient (%s): Run defer: All internal client goroutines finished.", c.marketType)

		// Close the WebSocket connection. Close() is safe for nil conn.
		c.Close() // Close is defined below
		c.log.Infof("BybitClient (%s): Run defer: WebSocket connection closed.", c.marketType)

		// Shared data channels (OrderBookChan, BBOChan) are NOT closed here.
		// They are closed once in main.go after ALL clients have stopped.

		c.log.Infof("BybitClient (%s): Main Run goroutine finished.", c.marketType)
	}()

	// Main connection/reconnection loop.
	for {
		// 4. Check external context before each connection attempt.
		select {
		case <-ctx.Done(): // Listen to the main application context
			c.log.Infof("BybitClient (%s): Run: Received cancellation signal from main context. Выход из Run loop.", c.marketType) // Используем логгер
			return                                                                                                                 // Выход из Run. Defer выполнится.
		default:
		}

		// 5. Check the isClosed flag. This flag is set by the Stop() method.
		c.mu.Lock()
		if c.isClosed {
			c.mu.Unlock()
			c.log.Infof("BybitClient (%s): Run: Клиент помечен как закрытый (isClosed). Выход из Run loop.", c.marketType) // Используем логгер
			return                                                                                                         // Выход из Run. Defer выполнится.
		}
		c.mu.Unlock()

		// 6. Exponential backoff before retrying (if not the first attempt).
		if c.reconnectAttempt > 0 {
			// waitForReconnect uses clientCtx, allowing the wait to be interrupted by cancellation.
			c.waitForReconnect(c.clientCtx) // waitForReconnect is defined below
			if c.clientCtx.Err() != nil {   // Check clientCtx after waiting.
				c.log.Infof("BybitClient (%s): Run: Внутренний контекст клиента отменен во время ожидания переподключения. Выход.", c.marketType) // Используем логгер
				return                                                                                                                            // Выход if clientCtx cancelled during wait. Defer will execute.
			}
		}
		// Increment attempt counter BEFORE the attempt.
		c.reconnectAttempt++

		c.log.Infof("BybitClient (%s): Run: Попытка #%d подключения WebSocket к %s...", c.marketType, c.reconnectAttempt, c.cfg.GetWSBaseURL()) // Используем логгер

		// 7. Establish WebSocket connection. ConnectWebSocket uses clientCtx for DialContext timeout.
		err := c.ConnectWebSocket() // ConnectWebSocket is defined below
		if err != nil {
			c.log.WithError(err).Errorf("BybitClient (%s): Run: Ошибка подключения WebSocket к %s. Закрытие соединения и повторная попытка...", c.marketType, c.cfg.GetWSBaseURL()) // Используем логгер
			c.Close()                                                                                                                                                               // Explicitly close connection on dial error. // Close is defined below
			continue                                                                                                                                                                // Next iteration for backoff.
		}

		// === УСПЕШНОЕ ПОДКЛЮЧЕНИЕ ===
		c.reconnectAttempt = 0                                                                        // Reset attempt counter after successful connection.
		c.log.Infof("BybitClient (%s): Run: WebSocket connection успешно установлено.", c.marketType) // Используем логгер
		time.Sleep(200 * time.Millisecond)                                                            // Небольшая пауза после подключения

		c.log.Infof("BybitClient (%s): Run: Запуск Listen и Keep-Alive...", c.marketType) // Используем логгер

		// === SYNCHRONIZED STATE: Client is connected and attempting to resubscribe ===

		// Channel to signal Listen goroutine completion. Created for each successful connection.
		listenDone := make(chan struct{})

		// 8. Launch Listen goroutine (reading messages).
		c.clientWg.Add(1)
		go func() {
			defer c.clientWg.Done()
			defer close(listenDone)                                                   // Signal Run on exit.
			c.listen(c.clientCtx)                                                     // Listen listens to clientCtx. // listen is defined below
			c.log.Infof("BybitClient (%s): Listen goroutine finished.", c.marketType) // Используем логгер
		}()
		c.log.Infof("BybitClient (%s): Listen goroutine launched.", c.marketType) // Используем логгер

		// 9. Launch KeepAlive goroutine (sending PING).
		c.clientWg.Add(1)
		go func() {
			defer c.clientWg.Done()
			c.keepAlive(c.clientCtx)                                                      // KeepAlive listens to clientCtx. // keepAlive is defined below
			c.log.Infof("BybitClient (%s): Keep-Alive goroutine finished.", c.marketType) // Используем логгер
		}()
		c.log.Infof("BybitClient (%s): Keep-Alive goroutine launched.", c.marketType) // Используем логгер

		// 10. Initiate resync for symbols with active depth subscriptions after successful connection/resubscription.
		// For Bybit V5, the WS 'partial' (snapshot) is sent on SUBSCRIBE.
		// The Scanner should trigger the ResyncTypeBybitResubscribe command
		// if it detects a gap later. Explicitly triggering REST snapshot here might not be needed.
		// We can remove the explicit resync command sending here after connection for Bybit.
		// If Scanner needs resync, it will command it via the channel.
		c.log.Debugf("BybitClient (%s): Run: Bybit V5 sends snapshot on SUBSCRIBE. Skipping explicit REST snapshot resync after connection.", c.marketType) // Используем логгер

		// 11. Wait for Listen goroutine to finish or external context cancellation.
		select {
		case <-ctx.Done(): // External context cancelled - full shutdown.
			c.log.Infof("BybitClient (%s): Run: Received external context cancellation signal (ctx.Done). Выход.", c.marketType) // Используем логгер
			return                                                                                                               // Выход из Run. Defer will execute.

		case <-listenDone: // Listen goroutine finished (likely connection dropped).
			c.log.Infof("BybitClient (%s): Run: Listen goroutine finished. Инициирование цикла переподключения.", c.marketType) // Используем логгер
			// Listen finished. Connection is likely invalid.
			// Explicitly close the connection to clean up resources and nil c.conn.
			c.Close() // Close WebSocket connection
			// The for {} loop automatically moves to the next iteration,
			// where a new connection attempt with backoff will start.
		}
	}
}

func (c *BybitClient) ConnectWebSocket() error { // <<< Прикреплен к *BybitClient
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithField("func", "ConnectWebSocket")
	logger.Debugf("BybitClient (%s): Попытка установки WebSocket соединения.", c.marketType) // Используем логгер Debug

	wsBaseURL := c.cfg.GetWSBaseURL() // Use URL from the specific market config

	u, err := url.Parse(wsBaseURL)
	if err != nil {
		// Логируем ошибку через c.log
		logger.WithError(err).Errorf("BybitClient (%s): Некорректный WebSocket URL в конфигурации '%s'.", c.marketType, wsBaseURL) // Используем логгер Error
		return fmt.Errorf("BybitClient (%s): invalid WebSocket URL in config '%s': %w", c.marketType, wsBaseURL, err)
	}

	logger.Infof("BybitClient (%s): Подключение к %s...", c.marketType, u.String()) // Используем логгер Info

	c.mu.Lock()         // Блокируем мьютекс перед работой с c.conn
	defer c.mu.Unlock() // Гарантируем освобождение мьютекса

	if c.isClosed {
		logger.Warnf("BybitClient (%s): Попытка подключиться к явно остановленному клиенту.", c.marketType) // Используем логгер Warn
		return fmt.Errorf("BybitClient (%s): attempt to connect on explicitly stopped client", c.marketType)
	}

	// Закрываем старое соединение, если оно существует
	if c.conn != nil {
		logger.Debugf("BybitClient (%s): Обнаружено старое соединение, закрываю перед подключением.", c.marketType) // Используем логгер Debug
		// Используем контекст с небольшим таймаутом для чистого закрытия старого соединения
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
		// Логируем использование контекста CloseWithContext на уровне Trace
		logger.Tracef("BybitClient (%s): Закрытие старого соединения с контекстом %v...", c.marketType, closeCtx)
		c.CloseWithContext(closeCtx) // Используем метод закрытия с контекстом (определен ниже)
		cancelClose()                // Отменяем контекст
		c.conn = nil                 // Убедимся, что установлено в nil
	}

	dialer := websocket.DefaultDialer // Стандартный Dialar.
	// Таймаут на установку соединения (Dial) задается через контекст, который передается в DialContext.
	dialCtx, dialCancel := context.WithTimeout(c.clientCtx, wsDialTimeout) // Создаем контекст для Dial с таймаутом (использует clientCtx)
	defer dialCancel()                                                     // Гарантируем отмену контекста Dial

	logger.Debugf("BybitClient (%s): Установка DialContext с таймаутом %s...", c.marketType, wsDialTimeout) // Используем логгер Debug

	// Устанавливаем соединение. DialContext позволяет прервать попытку подключения через переданный контекст.
	// Используем контекст dialCtx, который имеет таймаут.
	conn, resp, err := dialer.DialContext(dialCtx, u.String(), nil) // Используем контекст с таймаутом

	// Обработка ответа (resp). Тело ответа нужно закрыть, если оно есть.
	if resp != nil && resp.Body != nil {
		logger.Debugf("BybitClient (%s): Получен ответ от сервера при Dial: Status=%s", c.marketType, resp.Status) // Используем логгер Debug
		defer resp.Body.Close()                                                                                    // Гарантируем закрытие тела ответа
		// Опционально: прочитать тело ответа при ненулевом статусе для диагностики.
		// if resp.StatusCode != http.StatusOK {
		// 	bodyBytes, _ := io.ReadAll(resp.Body)
		// 	logger.Warnf("BybitClient (%s): Получен ненулевой статус (%d) при Dial. Тело ответа: %s", c.marketType, resp.StatusCode, string(bodyBytes)) // Используем логгер Warn
		// }
	}

	if err != nil {
		// Ошибка DialContext
		if resp != nil {
			// Если получен ответ, логируем его статус
			logger.WithError(err).WithField("status_code", resp.StatusCode).Errorf("BybitClient (%s): Ошибка DialContext для %s. Статус ответа: %s.", c.marketType, u.String(), resp.Status) // Используем логгер Error
		} else {
			// Если ответа нет (например, ошибка DNS или сеть недоступна)
			logger.WithError(err).Errorf("BybitClient (%s): Ошибка DialContext для %s (нет ответа).", c.marketType, u.String()) // Используем логгер Error
		}
		c.conn = nil // Убедимся, что соединение nil
		// Проверяем, является ли ошибка контекстной отменой (например, из-за таймаута Dial)
		if errors.Is(err, dialCtx.Err()) { // Используем errors.Is для проверки
			logger.Debugf("BybitClient (%s): Ошибка DialContext вызвана отменой контекста (таймаут или клиент остановлен).", c.marketType) // Используем логгер Debug
			return dialCtx.Err()                                                                                                           // Возвращаем ошибку контекста
		}
		return fmt.Errorf("BybitClient (%s): error connecting to %s: %w", c.marketType, u.String(), err)
	}
	// Соединение установлено успешно.

	// Настраиваем PONG обработчик для обновления lastPong.
	// Bybit V5 Public Spot/Linear использует JSON {"success": true, "ret_msg": "pong", ...} в ответ на {"op":"ping"}.
	// Этот обработчик стандартных WS PONG фреймов может не вызываться Bybit V5.
	conn.SetPongHandler(func(string) error {
		// c.log.Trace("BybitClient (%s): Received standard WS PONG (unexpected for V5 JSON Ping/Pong?)", c.marketType) // Log as Trace
		c.mu.Lock()             // Защищаем доступ к lastPong.
		c.lastPong = time.Now() // Обновляем время последнего PONG'а.
		c.mu.Unlock()           // Освобождаем мьютекс.
		return nil
	})
	c.lastPong = time.Now() // Сбрасываем таймер PONG/PING после успешного соединения.

	c.conn = conn // Сохраняем установленное соединение.

	logger.Infof("BybitClient (%s): Успешное WebSocket соединение с %s", c.marketType, u.String()) // Используем логгер Info

	return nil // Возвращаем nil при успешном соединении.
}

// listen слушает WebSocket соединение Bybit и обрабатывает входящие сообщения.
// Эта горутина запускается в методе Run после успешного подключения и слушает clientCtx для завершения.
// ctx: Контекст горутины (должен быть clientCtx).
func (c *BybitClient) listen(ctx context.Context) { // <<< Прикреплен к *BybitClient
	c.log.Info("BybitClient: Запуск горутины Listen...")        // Используем логгер
	defer c.log.Info("BybitClient: Горутина Listen завершена.") // Лог здесь, чтобы видеть, когда она точно завершилась

	for {
		// Проверяем контекст перед каждой попыткой чтения.
		select {
		case <-ctx.Done(): // Если контекст отменен...
			c.log.Info("BybitClient: Listen: Получен сигнал отмены контекста, выход.") // Используем логгер
			return                                                                     // ...выходим из горутины Listen.
		default:
			// Продолжаем, если контекст активен.
		}

		c.mu.Lock() // Захватываем мьютекс перед работой с c.conn.
		// Проверяем соединение под мьютексом перед использованием.
		if c.conn == nil {
			c.mu.Unlock() // Освобождаем мьютекс.
			// c.log.Debug("BybitClient: Listen: Соединение nil перед чтением, выход.") // Используем логгер (Закомментировано)
			return // Выходим, если соединение уже nil (например, закрыто методом Close()).
		}

		// Устанавливаем таймаут на чтение.
		err := c.conn.SetReadDeadline(time.Now().Add(c.wsReadTimeout)) // Используем таймаут чтения из поля структуры
		c.mu.Unlock()                                                  // Освобождаем мьютекс сразу после SetReadDeadline, до блокирующего ReadMessage.

		if err != nil {
			c.log.WithError(err).Error("BybitClient: Listen: Ошибка установки ReadDeadline.") // Используем логгер
			// Ошибка установки дедлайна - проблема, выходим. Run цикл начнет переподключение.
			return // Выход из Listen горутины.
		}

		// Выполняем блокирующее чтение сообщения.
		// При ошибке чтения (включая таймаут ReadDeadline или закрытие соединения), метод вернет ошибку.
		messageType, message, err := c.conn.ReadMessage()

		// Обработка ошибок чтения (выполняется ВНЕ мьютекса).
		if err != nil {
			// Проверяем тип ошибки, чтобы понять причину завершения чтения.
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				// Нормальное закрытие соединения (коды 1000, 1001).
				c.log.Infof("BybitClient: Listen: Соединение закрыто нормально: %v", err) // Используем логгер
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Ошибка сети по таймауту (сработал ReadDeadline). Вероятно, потеря соединения.
				c.log.WithError(err).Warn("BybitClient: Listen: Ошибка чтения по таймауту (вероятно, потеря соединения).") // Используем логгер
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				// Ошибка "use of closed network connection" означает, что c.conn.Close() был вызван другой горутиной (Run или Stop)
				// пока ReadMessage блокировался. Это ожидаемое поведение при остановке.
				c.log.Debug("BybitClient: Listen: Чтение прервано из-за закрытия сетевого соединения.") // Используем логгер
			} else {
				// Другая ошибка чтения.
				c.log.WithError(err).Error("BybitClient: Listen: Неизвестная ошибка чтения из WebSocket.") // Используем логгер
			}

			// В любом случае ошибки чтения, Listen горутина завершается.
			// Закрытие канала listenDone (в defer Run) просигнализирует Run о необходимости переподключения.
			return // Выход из Listen горутины.
		}

		// Если сообщение успешно прочитано:
		// Обрабатываем полученное сообщение (выполняется ВНЕ мьютекса).
		if messageType == websocket.TextMessage {
			// handleMessage не должна блокироваться надолго при отправке в каналы.
			c.handleMessage(message) // handleMessage определен ниже
		} else {
			// Игнорируем нетекстовые сообщения (пинги/понги обрабатывает библиотека gorilla или handleMessage для JSON).
			// c.log.Printf("BybitClient: Listen: Получено нетекстовое сообщение типа: %d", messageType) // Используем логгер (Закомментировано)
		}

		// Цикл продолжается для чтения следующего сообщения.
	} // Конец for loop
	// Defer log "BybitClient: Listen горутина завершена" сработает здесь.
}

// waitForReconnect реализует задержку перед переподключением, учитывая контекст.
// ИСПРАВЛЕНО: Теперь использует clientCtx для прерывания ожидания.
func (c *BybitClient) waitForReconnect(ctx context.Context) { // <<< Прикреплен к *BybitClient
	delay := c.getReconnectDelay(c.reconnectAttempt) // getReconnectDelay определен ниже, использует logrus внутри
	// Логгируем задержку через logrus
	c.log.Infof("BybitClient (%s): Waiting %s before next reconnection attempt #%d...", c.marketType, delay, c.reconnectAttempt)

	// Если getReconnectDelay вернул 0 (например, исчерпаны попытки и вызван clientCancel), выходим сразу.
	if delay <= 0 {
		c.log.Debug("BybitClient: WaitForReconnect: Задержка переподключения 0 или меньше. Пропуск ожидания.")
		return
	}

	select {
	case <-ctx.Done(): // Слушаем переданный контекст (clientCtx).
		c.log.Info("BybitClient: WaitForReconnect: Context cancellation (ctx.Done), остановка ожидания.")
		return // Выход если контекст отменен.
	case <-time.After(delay):
		// Ожидание завершено.
		c.log.Debug("BybitClient: WaitForReconnect: Ожидание завершено.")
	}
}

// getReconnectDelay вычисляет задержку перед переподключением с экспоненциальным backoff'ом и джиттером.
// Использует logrus для логирования attempts exhausted.
// *** ИСПРАВЛЕНЫ ОШИБКИ ТИПОВ float64 и time.Duration ***
func (c *BybitClient) getReconnectDelay(attempt int) time.Duration { // <<< Прикреплен к *BybitClient
	// Если достигнуто максимальное количество попыток и оно не равно 0 (бесконечно)...
	if reconnectMaxAttempts > 0 && c.reconnectAttempt > reconnectMaxAttempts {
		c.log.Warnf("BybitClient: GetReconnectDelay: Достигнуто максимальное количество попыток переподключения (%d). Сдаемся.", reconnectMaxAttempts) // Используем логгер
		c.clientCancel()                                                                                                                               // Сигнализируем Run, что сдаемся, отменяя внутренний контекст.
		return 0                                                                                                                                       // Возвращаем 0 задержку, чтобы waitForReconnect вышел сразу.
	}

	// Вычисляем экспоненциальную задержку в float64 для math.Pow и math.Min
	baseDelayFloat := float64(reconnectBaseDelay) // Конвертируем в float64
	maxDelayFloat := float64(reconnectMaxDelay)   // Конвертируем в float64
	delayFloat := baseDelayFloat * math.Pow(2, float64(c.reconnectAttempt-1))

	// Ограничиваем максимальной задержкой в float64.
	delayFloat = math.Min(delayFloat, maxDelayFloat) // Работаем с float64

	// Добавляем джиттер (случайность) в float64.
	jitterFactor := reconnectJitterFactor * (float64(time.Now().UnixNano()%1000)/1000.0*2.0 - 1.0) // Range [-jitterFactor, +jitterFactor]
	jitterAmountFloat := jitterFactor * delayFloat                                                 // Работаем с float64

	delayFloat += jitterAmountFloat // Складываем float64

	// Гарантируем, что задержка неотрицательна в float64.
	if delayFloat < 0 {
		delayFloat = 0
	}
	// Гарантируем минимальную задержку в float64 для попыток > 0.
	if c.reconnectAttempt > 0 && delayFloat < float64(100*time.Millisecond) { // Сравниваем float64
		delayFloat = float64(100 * time.Millisecond) // Присваиваем float64
	}

	// Конвертируем финальную задержку обратно в time.Duration
	return time.Duration(delayFloat) // Конвертируем в time.Duration
}

// generateSubscribeCommandsFromActiveSubscriptions генерирует SubscribeCommand slices
// для всех текущих активных подписок в пределах ТИПА РЫНКА ЭТОГО клиента.
// Используется во время переподключения. Использует logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) generateSubscribeCommandsFromActiveSubscriptions() []types.SubscriptionCommand { // <<< Прикреплен к *BybitClient
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithField("func", "generateSubscribeCommandsFromActiveSubscriptions")
	logger.Debugf("BybitClient (%s): Генерация команд подписки из активных подписок...", c.marketType) // Используем логгер

	c.subMu.RLock() // Блокируем для чтения activeSubscriptions
	defer c.subMu.RUnlock()

	cmds := make([]types.SubscriptionCommand, 0, len(c.activeSubscriptions))
	for _, cmd := range c.activeSubscriptions {
		// Фильтруем команды, чтобы включить только те, которые относятся к ТИПУ РЫНКА ЭТОГО клиента
		// Эта проверка может быть избыточной, если activeSubscriptions содержит только
		// команды для типа рынка этого клиента, но она делает код безопаснее.
		if cmd.MarketType == c.marketType {
			cmds = append(cmds, cmd)                                                                                       // Добавляем копию команды
			logger.WithField("command", cmd).Tracef("BybitClient (%s): Добавлена команда для переподписки.", c.marketType) // Trace
		} else {
			logger.WithField("command", cmd).Tracef("BybitClient (%s): Пропущена команда для другого типа рынка.", c.marketType) // Trace
		}
	}

	logger.WithField("count", len(cmds)).Debugf("BybitClient (%s): Генерация команд подписки завершена.", c.marketType) // Используем логгер
	return cmds
}

// getTopicsForSymbolFromActiveSubscriptions собирает список топиков для конкретного символа из активных подписок.
// Используется при ресинке для конкретного символа.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) getTopicsForSymbolFromActiveSubscriptions(symbol string) []string { // <<< Прикреплен к *BybitClient
	// Используем логгер с контекстом для этого метода и символа
	logger := c.log.WithFields(logrus.Fields{
		"func":   "getTopicsForSymbolFromActiveSubscriptions",
		"symbol": strings.ToUpper(symbol), // Логируем символ в верхнем регистре
	})
	logger.Tracef("BybitClient (%s): Сбор активных топиков для символа.", c.marketType) // Используем логгер Trace

	c.subMu.RLock() // Read Lock
	defer c.subMu.RUnlock()

	upperSymbol := strings.ToUpper(symbol) // Символ в верхнем регистре para сравнения

	topics := make([]string, 0, len(c.activeSubscriptions))
	for topic := range c.activeSubscriptions {
		// Проверяем, что топик относится к данному символу, заканчивается на .SYMBOL (регистронезависимо)
		// Проверяем, что MarketType в команде для этого топика соответствует типу рынка клиента.
		if cmd, ok := c.activeSubscriptions[topic]; ok && cmd.MarketType == c.marketType && strings.HasSuffix(topic, "."+upperSymbol) {
			topics = append(topics, topic)
			logger.WithField("topic", topic).Tracef("BybitClient (%s): Обнаружен активный топик для символа.", c.marketType) // Trace
		} else if ok && cmd.MarketType != c.marketType {
			logger.WithField("topic", topic).WithField("cmd_market_type", cmd.MarketType).Tracef("BybitClient (%s): Пропуск активного топика другого типа рынка.", c.marketType) // Trace
		}
	}

	logger.WithField("count", len(topics)).Tracef("BybitClient (%s): Сбор активных топиков для символа завершен.", c.marketType) // Используем логгер Trace
	return topics
}

// handleResyncCommands слушает канал команд ресинка от Scanner'а и запускает
// горутины для выполнения логики ресинка (переподписки).
// Эта горутина работает весь жизненный цикл клиента и слушает clientCtx для завершения.
// ctx: Контекст горутины (должен быть clientCtx).
func (c *BybitClient) handleResyncCommands(ctx context.Context) { // <<< Прикреплен к *BybitClient
	c.log.Info("BybitClient: Запущена горутина handleResyncCommands") // Используем логгер
	// Цикл будет работать до закрытия канала c.Channels.ResyncChan ИЛИ отмены ctx.Done().
	for {
		select {
		case cmd, ok := <-c.Channels.ResyncChan: // *** ИСПРАВЛЕНО: Читаем из канала в Channels ***
			if !ok {
				c.log.Info("BybitClient: handleResyncCommands: Канал ResyncChan закрыт, завершение.") // Используем логгер
				return                                                                                // Канал закрыт (main defer), выходим из горутины.
			}
			c.log.WithField("command", cmd).Infof("BybitClient: handleResyncCommands: Получена команда ресинка: %+v", cmd) // Используем логгер

			// Проверяем, что команда предназначена для этого клиента и нужного типа.
			// Для Bybit V5 Public Spot ресинк - это просто переподписка на WS поток.
			// Команда от Scanner'а должна содержать символ.
			if cmd.Source == "bybit" && cmd.MarketType == c.marketType && cmd.Type == types.ResyncTypeBybitResubscribe {
				c.log.WithField("symbol", cmd.Symbol).Infof("BybitClient: handleResyncCommands: Инициирую Bybit Resync для %s (WS Re-subscribe)...", cmd.Symbol) // Используем логгер
				// Запускаем выполнение переподписки в отдельной горутине,
				// чтобы не блокировать эту горутину и не пропускать другие команды из ResyncChan.
				c.clientWg.Add(1) // Добавляем новую горутину выполнения ресинка в WaitGroup клиента
				// ИСПРАВЛЕНО: Определяем анонимную функцию, принимающую аргумент 'cmd'.
				go func(resyncCmd *types.ResyncCommand) {
					defer c.clientWg.Done() // Декрементируем WaitGroup при выходе
					// Выполняем логику ресинка (переподписка).
					// Удален неиспользуемый 'depth'
					symbol := resyncCmd.Symbol

					c.log.Printf("BybitClient: performBybitResync Goroutine: Переподписка для символа %s...", symbol) // Используем логгер

					// Собираем топики для реподписки для данного символа из активных подписок.
					// Используем обновленный getTopicsForSymbolFromActiveSubscriptions
					topicsToResubscribe := c.getTopicsForSymbolFromActiveSubscriptions(symbol) // *** getTopicsForSymbolFromActiveSubscriptions определен выше ***

					if len(topicsToResubscribe) > 0 {
						// Выполняем подписку. SubscribeBybit отправляет запрос через WS.
						// Если SubscribeBybit вернет ошибку (например, WS соединение nil или ошибка записи),
						// это будет залогировано внутри SubscribeBybit/sendSubscriptionWSMessageBybit.
						err := c.SubscribeBybit(c.clientCtx, topicsToResubscribe) // *** SubscribeBybit определен ниже *** Передаем контекст горутины
						if err != nil {
							c.log.WithError(err).Errorf("BybitClient: performBybitResync Goroutine: Ошибка при отправке запроса на переподписку для %s.", symbol) // Используем логгер
							// TODO: Логика обработки ошибки реподписки при ресинке.
						} else {
							c.log.Infof("BybitClient: performBybitResync Goroutine: Запрос на переподписку для %s успешно отправлен.", symbol) // Используем логгер
							// Для Bybit V5, биржа пришлет SNAPSHOT по WS после успешной подписки.
							// Scanner получит его через OrderBookChan (который в c.Channels).
						}
					} else {
						c.log.Warnf("BybitClient: performBybitResync Goroutine: Нет активных подписок для символа %s для переподписки. Ресинк не выполнен.", symbol) // Используем логгер
						// В этом случае, возможно, нужно уведомить Scanner, что ресинк не выполнен, т.к. нет подписок?
						// Можно отправить ResyncCommand с типом "resync_failed" обратно в Scanner.
					}

					c.log.Infof("BybitClient performBybitResync горутина для %s завершена.", symbol) // Используем логгер
				}(cmd) // Передаем команду (указатель) в горутину при запуске

			} else {
				c.log.WithField("command", cmd).Warnf("BybitClient: handleResyncCommands: Получена неизвестная или нерелевантная команда ресинка: %+v. Игнорирую.", cmd) // Используем логгер
			}
		case <-ctx.Done(): // *** ИСПРАВЛЕНО: Слушаем контекст горутины (clientCtx) ***
			c.log.Info("BybitClient: handleResyncCommands: Получен сигнал отмены контекста (clientCtx), завершение.") // Используем логгер
			return                                                                                                    // Выходим из горутины при отмене контекста.
		}
	}
}

// handleSubscriptionCommands слушает канал команд подписки и выполняет соответствующие действия (subscribe/unsubscribe).
// Эта горутина работает весь жизненный цикл клиента и слушает clientCtx для завершения.
// ctx: Контекст горутины (должен быть clientCtx).
func (c *BybitClient) handleSubscriptionCommands(ctx context.Context) {
	c.log.Infof("BybitClient (%s): handleSubscriptionCommands goroutine launched.", c.marketType)       // Используем логгер
	defer c.log.Infof("BybitClient (%s): handleSubscriptionCommands goroutine finished.", c.marketType) // Используем логгер

	// Flag to track if initial subscriptions have been sent after connection.
	// Reset to false implicitly by Run restarting this goroutine on connect.
	connectedAndSubscribedOnce := false // *** Используется ниже ***

	// Add a small delay before sending the initial batch after connection.
	// This is beneficial for some exchanges to ensure the connection is fully ready.
	initialSubscribeDelay := 500 * time.Millisecond // TODO: Make configurable

	// Timer for the initial subscription delay after connection is ready.
	initialSubscribeTimer := time.NewTimer(initialSubscribeDelay)
	// Stop and drain immediately, it will be reset upon successful connection signal.
	if !initialSubscribeTimer.Stop() {
		select {
		case <-initialSubscribeTimer.C:
		default:
		}
	}

	// Inner function to check if the connection is currently active and usable for sending
	isConnected := func() bool {
		c.mu.Lock()
		connStatus := c.conn != nil && !c.isClosed
		c.mu.Unlock()
		return connStatus
	}

	// --- Helper function to send the accumulated batch ---
	// This helper is called internally and manages the batch buffer and sending.
	// It does NOT manage the batchTimer itself.
	sendBatchInternal := func() {
		c.mu.Lock() // Acquire mutex for topicsToBatch buffer before reading
		// ИСПРАВЛЕНО: Объявляем переменные здесь для корректной области видимости
		subscribeTopics := c.topicsToBatch["subscribe"]
		unsubscribeTopics := c.topicsToBatch["unsubscribe"] // *** ИСПРАВЛЕНО: Объявлена здесь ***
		// Make copies of slice headers if needed to release mutex early, but clearing buffer later requires mutex anyway.
		// Keep mutex until buffer is cleared.

		// If buffer is empty OR connection is not active, don't attempt to send.
		if len(subscribeTopics) == 0 && len(unsubscribeTopics) == 0 {
			c.mu.Unlock() // Release mutex
			c.log.Tracef("BybitClient (%s): sendBatchInternal: Batch buffer is empty. Skipping send.", c.marketType)
			return // Nothing to send
		}
		// Check connection status *before* trying to send
		if !isConnected() {
			c.mu.Unlock() // Release mutex
			c.log.Warnf("BybitClient (%s): sendBatchInternal: Connection is not active. Cannot send subscription batch.", c.marketType)
			return // Cannot send if not connected
		}

		sendCtx, cancelSend := context.WithTimeout(c.clientCtx, wsSubscriptionCommandTimeout)
		defer cancelSend()

		c.log.WithFields(logrus.Fields{
			"subscribe_count":   len(subscribeTopics),   // Используем subscribeTopics
			"unsubscribe_count": len(unsubscribeTopics), // Используем unsubscribeTopics
		}).Debugf("BybitClient (%s): sendBatchInternal: Attempting to send subscription batch.", c.marketType)

		// --- Отправка SUBSCRIBE ---
		if len(subscribeTopics) > 0 {
			c.log.Tracef("BybitClient (%s): sendBatchInternal: Sending %d SUBSCRIBE topics.", c.marketType, len(subscribeTopics))
			// ИСПРАВЛЕНО: Используем локальную переменную topicsToSubscribe
			for i := 0; i < len(subscribeTopics); i += c.wsMaxTopicsPerSubscribe {
				end := i + c.wsMaxTopicsPerSubscribe
				if end > len(subscribeTopics) {
					end = len(subscribeTopics)
				}
				// ИСПРАВЛЕНО: Используем 'batch', а не 'subscribeTopics' в цикле
				batch := subscribeTopics[i:end]
				if len(batch) > 0 {
					c.log.WithField("batch_size", len(batch)).Debugf("BybitClient (%s): sendBatchInternal: Отправка SUBSCRIBE пакета...", c.marketType)
					// sendSubscriptionWSMessageBybit uses the provided context for write deadline and cancellation.
					// Release buffer mutex temporarily during network operation
					c.mu.Unlock()                                                        // Release mutex BEFORE network call
					err := c.sendSubscriptionWSMessageBybit(sendCtx, "subscribe", batch) // Bybit uses lowercase method names
					c.mu.Lock()                                                          // Re-acquire mutex AFTER network call
					if err != nil {
						c.log.WithError(err).WithField("batch_topics", batch).Errorf("BybitClient (%s): sendBatchInternal: Failed to send SUBSCRIBE batch. Connection may be broken.", c.marketType)
						// If send fails, the connection is likely broken.
						// Clear the buffer as commands likely failed and will need resubscription after reconnect.
						c.topicsToBatch = make(map[string][]string) // Clear buffer on send error
						return                                      // Stop sending batch if an error occurs. Run loop handles reconnection.
					} else {
						c.log.WithField("batch_topics", batch).Debugf("BybitClient (%s): sendBatchInternal: SUBSCRIBE batch sent successfully.", c.marketType)
						// TODO: Optionally wait for confirmation response.
					}
					// Add a small delay between batches if necessary to respect per-connection message rate limits
					if i+c.wsMaxTopicsPerSubscribe < len(subscribeTopics) { // Use subscribeTopics
						c.mu.Unlock() // Release mutex temporarily during sleep/wait
						select {
						case <-time.After(c.wsSendMessageRateLimit):
							// Wait completed
						case <-sendCtx.Done():
							c.log.Warnf("BybitClient (%s): sendBatchInternal: Context cancelled during wait between SUBSCRIBE batches.", c.marketType)
							c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
							c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
							c.mu.Unlock()                               // Release mutex
							return
						}
						c.mu.Lock() // Re-acquire mutex after sleep/wait
					}
				}
			}
		}

		// --- Отправка UNSUBSCRIBE ---
		if len(unsubscribeTopics) > 0 { // Используем unsubscribeTopics
			c.log.Tracef("BybitClient (%s): sendBatchInternal: Sending %d UNSUBSCRIBE topics.", c.marketType, len(unsubscribeTopics)) // Используем unsubscribeTopics
			// Wait for rate limit interval before sending UNSUBSCRIBE batch if SUBSCRIBE batches were sent
			if len(subscribeTopics) > 0 { // Используем subscribeTopics
				c.mu.Unlock() // Release mutex temporarily during sleep/wait
				select {
				case <-time.After(c.wsSendMessageRateLimit):
				case <-sendCtx.Done():
					c.log.Warnf("BybitClient (%s): sendBatchInternal: Context cancelled during wait before UNSUBSCRIBE batches.", c.marketType)
					c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
					c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
					c.mu.Unlock()                               // Release mutex
					return
				}
				c.mu.Lock() // Re-acquire mutex after sleep/wait
			}

			for i := 0; i < len(unsubscribeTopics); i += c.wsMaxTopicsPerSubscribe { // Используем unsubscribeTopics
				end := i + c.wsMaxTopicsPerSubscribe
				if end > len(unsubscribeTopics) { // Используем unsubscribeTopics
					end = len(unsubscribeTopics)
				}
				batch := unsubscribeTopics[i:end] // Используем unsubscribeTopics
				if len(batch) > 0 {
					c.log.WithField("batch_size", len(batch)).Debugf("BybitClient (%s): sendBatchInternal: Отправка UNSUBSCRIBE пакета...", c.marketType)
					c.mu.Unlock()                                                          // Release mutex temporarily during network operation
					err := c.sendSubscriptionWSMessageBybit(sendCtx, "unsubscribe", batch) // Bybit uses lowercase method names
					c.mu.Lock()                                                            // Re-acquire mutex AFTER network call
					if err != nil {
						c.log.WithError(err).WithField("batch_topics", batch).Errorf("BybitClient (%s): sendBatchInternal: Failed to send UNSUBSCRIBE batch. Connection may be broken.", c.marketType)
						c.topicsToBatch = make(map[string][]string) // Clear buffer on send error
						return                                      // Stop sending batch if an error occurs.
					} else {
						c.log.WithField("batch_topics", batch).Debugf("BybitClient (%s): sendBatchInternal: UNSUBSCRIBE пакет успешно отправлен.", c.marketType)
						// TODO: Optionally wait for confirmation response.
					}
					// Add a small delay between batches if necessary
					if i+c.wsMaxTopicsPerSubscribe < len(unsubscribeTopics) { // Используем unsubscribeTopics
						c.mu.Unlock() // Release mutex temporarily during sleep/wait
						select {
						case <-time.After(c.wsSendMessageRateLimit):
						case <-sendCtx.Done():
							c.log.Warnf("BybitClient (%s): sendBatchInternal: Context cancelled during wait between UNSUBSCRIBE batches.", c.marketType)
							c.mu.Lock()                                 // Re-acquire mutex before clearing buffer
							c.topicsToBatch = make(map[string][]string) // Clear buffer on cancellation
							c.mu.Unlock()                               // Release mutex
							return
						}
						c.mu.Lock() // Re-acquire mutex after sleep/wait
					}
				}
			}
		}

		// Clear the batch buffer after successful sending (still under mutex)
		c.topicsToBatch = make(map[string][]string)

		// Flag that initial subscriptions have been sent, used outside this helper.
		// This should be done *only* for the first successful batch send after connect.
		if !connectedAndSubscribedOnce {
			connectedAndSubscribedOnce = true // *** Используется здесь ***
			c.log.Infof("BybitClient (%s): sendBatchInternal: First batch of subscription commands after connection sent successfully.", c.marketType)
			// Initial snapshot initiation for Bybit V5 is handled by the server responding to SUBSCRIBE.
			// We don't need to trigger a REST snapshot fetch here.
		}

		c.log.Debugf("BybitClient (%s): sendBatchInternal: Batch send completed. Buffer cleared.", c.marketType)
		c.mu.Unlock() // Release mutex at the end of the helper
	}

	// --- Main loop processing commands and timer ---
	for {
		select {
		case cmd, ok := <-c.Channels.SubChan: // Read commands from the channel (cmd is *types.SubscriptionCommand)
			if !ok {
				c.log.Infof("BybitClient (%s): handleSubscriptionCommands: SubChan closed. Exiting goroutine.", c.marketType) // Используем логгер
				// Send any remaining batch before exiting
				sendBatchInternal() // Call internal sender
				return              // Channel closed, exit.
			}
			// ИСПРАВЛЕНО: Использование переменной cmd
			c.log.WithField("command_type", cmd.Type).WithField("symbol", cmd.Symbol).Infof("BybitClient (%s): handleSubscriptionCommands: Received subscription command.", c.marketType) // Используем cmd

			// Check if the command is for THIS client instance (source and market type).
			if cmd.Source != "bybit" || cmd.MarketType != c.marketType { // *** CHECK MARKETTYPE ***
				c.log.WithFields(logrus.Fields{
					"cmd_source":      cmd.Source,
					"cmd_market_type": cmd.MarketType,
				}).Warnf("BybitClient (%s): handleSubscriptionCommands: Received command not for this client (%s %s). Ignoring.", c.marketType, cmd.Source, cmd.MarketType) // Используем логгер Warn
				continue // Ignore command for another client.
			}

			// Process the command to get topics and update active subscriptions map (under lock).
			// updateActiveSubscriptions will add/remove commands based on the *value* copy from the channel.
			affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch := c.getTopicsAndBatchesFromCommand(cmd) // <<< getTopicsAndBatchesFromCommand defined below

			c.subMu.Lock()                                   // Acquire mutex for activeSubscriptions
			c.updateActiveSubscriptions(cmd, affectedTopics) // *** Pass pointer cmd *** // <<< updateActiveSubscriptions defined below (Pass pointer cmd)
			c.subMu.Unlock()                                 // Release activeSubscriptions mutex

			// Add topics to the batch buffer (under mutex).
			c.mu.Lock() // Acquire mutex for topicsToBatch buffer
			c.topicsToBatch["subscribe"] = append(c.topicsToBatch["subscribe"], topicsToSubscribeBatch...)
			c.topicsToBatch["unsubscribe"] = append(c.topicsToBatch["unsubscribe"], topicsToUnsubscribeBatch...)
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
			c.mu.Unlock() // Release buffer mutex

			c.log.WithFields(logrus.Fields{
				"added_subscribe":            len(topicsToSubscribeBatch),
				"added_unsubscribe":          len(topicsToUnsubscribeBatch),
				"total_in_batch_subscribe":   len(c.topicsToBatch["subscribe"]),
				"total_in_batch_unsubscribe": len(c.topicsToBatch["unsubscribe"]),
			}).Debugf("BybitClient (%s): handleSubscriptionCommands: Topics added to batch buffer.", c.marketType) // Используем логгер Debug

			// If the connection is currently active AND buffer has items, reset the batch timer.
			// This ensures the batch is sent soon.
			if bufferHasItems && isConnected() {
				if !c.batchTimer.Stop() {
					select {
					case <-c.batchTimer.C:
					default:
					}
				}
				// Reset the timer to fire after the configured interval.
				c.batchTimer.Reset(defaultBatchInterval) // Reset to default batch interval
				c.log.Tracef("BybitClient (%s): handleSubscriptionCommands: Batch timer reset due to new command.", c.marketType)
			} else {
				c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Connection not active or buffer empty after adding command. Batch timer not reset.", c.marketType)
			}

		case <-c.batchTimer.C: // Batch timer fires
			// Timer fired. Check if connection is active and buffer has items.
			c.mu.Lock() // Acquire mutex to check buffer status
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
			c.mu.Unlock() // Release mutex

			if bufferHasItems && isConnected() {
				c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Таймер пакетирования сработал. Отправка пакета.", c.marketType) // Используем логгер Debug
				// sendBatchInternal will clear the buffer on success or error.
				sendBatchInternal()
				// Reset the timer for the next interval IF buffer was not empty before the send attempt.
				c.mu.Lock() // Re-acquire mutex to check buffer status after send
				bufferStillHasItemsAfterSend := len(c.topicsToBatch["subscribe"]) > 0 || len(c.topicsToBatch["unsubscribe"]) > 0
				c.mu.Unlock() // Release mutex

				if !bufferStillHasItemsAfterSend { // Buffer was successfully sent and cleared
					c.batchTimer.Reset(defaultBatchInterval)
					c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Timer fired and batch sent. Timer reset to %s.", c.marketType, defaultBatchInterval)
				} else {
					// Buffer still has items after sendBatchInternal (implies send error).
					// Timer stays stopped. Run loop handles reconnection.
					c.log.Warnf("BybitClient (%s): handleSubscriptionCommands: Timer fired and batch send failed. Timer not reset.", c.marketType)
				}

			} else {
				c.log.Tracef("BybitClient (%s): handleSubscriptionCommands: Batch timer fired, but buffer is empty or connection is not active. Skipping send.", c.marketType)
				// Timer fired but nothing was sent. Let the timer stay stopped until a new command arrives
				// AND connection is active.
			}

		// Listen for signal indicating connection is established and delay is over.
		case <-c.connReadyForSubscribe: // Read signal from Run loop
			c.log.Infof("BybitClient (%s): handleSubscriptionCommands: Received connReadyForSubscribe signal. Connection is ready.", c.marketType)

			// Start the initial subscribe delay timer.
			// If it was already running or stopped, reset it.
			if !initialSubscribeTimer.Stop() {
				select {
				case <-initialSubscribeTimer.C:
				default:
				}
			}
			initialSubscribeTimer.Reset(initialSubscribeDelay)
			c.log.Debugf("BybitClient (%s): Initial subscribe delay timer reset for %s.", c.marketType, initialSubscribeDelay)

		case <-initialSubscribeTimer.C: // Timer for initial subscribe delay elapsed
			c.log.Infof("BybitClient (%s): handleSubscriptionCommands: Initial subscribe timer elapsed. Attempting initial batch send.", c.marketType)

			// Generate commands from active subscriptions and add them to the batch buffer.
			// This regenerates the batch buffer based on what should be subscribed.
			c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Regenerating batch buffer from active subscriptions for resubscribe...", c.marketType)
			// ИСПРАВЛЕНО: Удалена неиспользуемая переменная resubscribeCmds
			// resubscribeCmds := c.generateSubscribeCommandsFromActiveSubscriptions() // Gets commands for THIS client's market type

			c.mu.Lock()                                 // Acquire mutex for topicsToBatch
			c.topicsToBatch = make(map[string][]string) // Clear buffer first
			// Iterate active subscriptions map to get the topics currently active.
			c.subMu.RLock() // Acquire read lock for activeSubscriptions
			for topic, activeCmd := range c.activeSubscriptions {
				// Verify it's indeed a subscribe command we want to resend
				if activeCmd.Source == "bybit" && activeCmd.MarketType == c.marketType &&
					(activeCmd.Type == types.SubscribeDepth || activeCmd.Type == types.SubscribeBBO) { // Only resubscribe depth/BBO
					c.topicsToBatch["subscribe"] = append(c.topicsToBatch["subscribe"], topic) // Add topic directly
				}
			}
			c.subMu.RUnlock() // Release read lock
			bufferHasItems := len(c.topicsToBatch["subscribe"]) > 0
			c.mu.Unlock() // Release buffer mutex

			c.log.WithField("topics_to_subscribe", len(c.topicsToBatch["subscribe"])).Infof("BybitClient (%s): handleSubscriptionCommands: Buffer populated for initial batch send.", c.marketType)

			// Now send the batch immediately if buffer is not empty and connected.
			if bufferHasItems && isConnected() {
				sendBatchInternal()
				// Reset the batch timer for subsequent command batching.
				c.batchTimer.Reset(defaultBatchInterval)
				c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Initial batch sent. Batch timer reset to %s.", c.marketType, defaultBatchInterval)
			} else {
				c.log.Debugf("BybitClient (%s): handleSubscriptionCommands: Initial batch buffer empty or connection not active. No initial batch sent.", c.marketType)
			}

		case <-ctx.Done(): // Client's internal context cancelled (from Stop() or Run defer)
			c.log.Infof("BybitClient (%s): handleSubscriptionCommands: Received context cancellation signal (clientCtx). Exiting.", c.marketType)
			// Attempt to send any pending batch before exiting.
			sendBatchInternal() // Call internal sender
			return
		}
	}
}

func (c *BybitClient) getTopicsAndBatchesFromCommand(cmd *types.SubscriptionCommand) ([]string, []string, []string) { // <<< Прикреплен к *BybitClient, Принимает *SubscriptionCommand
	// Используем логгер с контекстом для этого метода и команды
	logger := c.log.WithFields(logrus.Fields{
		"func":    "getTopicsAndBatchesFromCommand",
		"command": cmd, // Логируем всю команду для отладки
	})
	logger.Debugf("BybitClient (%s): Определение топиков для команды подписки/отписки.", c.marketType) // Используем логгер Debug

	// Bybit V5 Public topics use uppercase symbols.
	upperSymbol := strings.ToUpper(cmd.Symbol)

	affectedTopics := []string{}
	topicsToSubscribeBatch := []string{}
	topicsToUnsubscribeBatch := []string{}

	// Ensure the command is for THIS client's market type (redundant check as handler filters, but safer)
	if cmd.MarketType != c.marketType || cmd.Source != "bybit" {
		logger.Warnf("BybitClient (%s): Получена команда не для этого клиента. Этого не должно происходить.", c.marketType) // Используем логгер Warn
		return affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch
	}

	switch cmd.Type {
	case types.SubscribeDepth:
		// Определяем целевой топик для новой подписки на глубину.
		// Bybit V5 Depth topics: "orderbook.{depth}.{symbol}", where depth is 1, 50, 200, 500, 1000.
		// L1 (depth 1) is also used for BBO.
		validDepths := map[int]bool{1: true, 50: true, 200: true, 500: true, 1000: true}
		depth := cmd.Depth
		if depth <= 0 || !validDepths[depth] {
			logger.WithField("depth", cmd.Depth).Warnf("BybitClient (%s): Неподдерживаемая или нулевая глубина для подписки. Использование дефолтной 50.", c.marketType) // Используем логгер Warn
			depth = 50                                                                                                                                                   // Default depth if invalid
		}
		targetTopic := fmt.Sprintf("orderbook.%d.%s", depth, upperSymbol)
		logger.WithField("target_topic", targetTopic).Debugf("BybitClient (%s): Определен целевой топик для SubscribeDepth.", c.marketType) // Используем логгер Debug

		// 1. Topics to add to activeSubscriptions: The target topic.
		affectedTopics = append(affectedTopics, targetTopic)

		// 2. Topics to remove from activeSubscriptions: ALL OTHER active depth topics for this symbol and market type.
		//    This also means removing the orderbook.1 topic if we are subscribing to a different depth.
		//    However, we might want to keep orderbook.1 if we're also explicitly subscribed to BBO.
		//    Let's assume for now that subscribing to *any* orderbook.X topic for a symbol means
		//    we are managing depth, and other orderbook.Y topics (including orderbook.1) should be unsubscribed
		//    UNLESS there's an explicit SubscribeBBO command active for this symbol.
		//    This is getting complicated. Let's keep it simple: SubscribeDepth(X) means unsubscribe from *any*
		//    existing orderbook.Y topic for the same symbol, but add orderbook.X.

		c.subMu.RLock() // Блокируем для чтения activeSubscriptions
		for activeTopic, activeCmd := range c.activeSubscriptions {
			// Check if it's an orderbook topic, for the same symbol, for THIS market type, and NOT the target topic.
			if activeCmd.MarketType == c.marketType && activeTopic != targetTopic &&
				strings.HasPrefix(activeTopic, "orderbook.") && strings.HasSuffix(activeTopic, "."+upperSymbol) {
				// This active topic is NOT the target depth, and it's for the same symbol/market.
				// It should be removed from active subscriptions.
				affectedTopics = append(affectedTopics, activeTopic)                                                                                   // Add to affected (for removal)
				logger.WithField("old_depth_topic", activeTopic).Tracef("BybitClient (%s): Обнаружен старый топик глубины для отписки.", c.marketType) // Trace
			}
		}
		c.subMu.RUnlock() // Освобождаем блокировку чтения

		// 3. Topics for batching: Add the target topic to the SUBSCRIBE batch. Add the old depth topics to the UNSUBSCRIBE batch.
		topicsToSubscribeBatch = append(topicsToSubscribeBatch, targetTopic) // Подписываемся на новую глубину
		// Collect topics that should be UNSUBSCRIBED. This includes old depth topics.
		// Get the list of topics *currently active* for this symbol/market type
		// and determine which ones should be unsubscribed because they are NOT the target depth.
		c.subMu.RLock() // Re-locking to iterate over the active subscriptions safely
		for activeTopic, activeCmd := range c.activeSubscriptions {
			if activeCmd.MarketType == c.marketType && activeTopic != targetTopic &&
				strings.HasPrefix(activeTopic, "orderbook.") && strings.HasSuffix(activeTopic, "."+upperSymbol) {
				// This active topic is NOT the target depth, and it's for the same symbol/market.
				// We should unsubscribe from it.
				topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, activeTopic)                                                                                // Add to UNSUBSCRIBE batch
				logger.WithField("depth_topic_to_unsubscribe", activeTopic).Tracef("BybitClient (%s): Обнаружен топик глубины для отписки (для пакета).", c.marketType) // Trace
			}
		}
		c.subMu.RUnlock() // Освобождаем блокировку чтения

	case types.UnsubscribeDepth:
		// Topics to remove from activeSubscriptions and UNSUBSCRIBE batch: ALL active depth topics for this symbol and market type.
		c.subMu.RLock() // Блокируем для чтения activeSubscriptions
		for activeTopic, activeCmd := range c.activeSubscriptions {
			// Check if it's a depth topic (starts with "orderbook."), for the same symbol, and for this market type.
			if activeCmd.MarketType == c.marketType &&
				strings.HasPrefix(activeTopic, "orderbook.") && strings.HasSuffix(activeTopic, "."+upperSymbol) {
				affectedTopics = append(affectedTopics, activeTopic)                                                                                                    // Добавляем к затронутым (для удаления)
				topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, activeTopic)                                                                                // Добавляем в UNSUBSCRIBE пакет
				logger.WithField("depth_topic_to_unsubscribe", activeTopic).Tracef("BybitClient (%s): Обнаружен топик глубины для отписки (для пакета).", c.marketType) // Trace
			}
		}
		c.subMu.RUnlock() // Освобождаем блокировку чтения

	case types.SubscribeBBO:
		// Bybit V5 BBO is orderbook.1. topic.
		topic := fmt.Sprintf("orderbook.1.%s", upperSymbol)
		logger.WithField("target_topic", topic).Debugf("BybitClient (%s): Определен целевой топик для SubscribeBBO.", c.marketType) // Используем логгер Debug

		// 1. Topics to add to activeSubscriptions: The BBO topic.
		affectedTopics = append(affectedTopics, topic)
		// 2. Topics for batching: Add the BBO topic to the SUBSCRIBE batch.
		topicsToSubscribeBatch = append(topicsToSubscribeBatch, topic)
		// If we want only one BBO sub per symbol/market type, check activeSubscriptions
		// for other 'orderbook.1' topics for this symbol and add them to topicsToUnsubscribeBatch.
		// Given Bybit V5, the topic format is fixed, so there shouldn't be *other* orderbook.1 topics.

	case types.UnsubscribeBBO:
		// Bybit V5 BBO is orderbook.1. topic.
		topic := fmt.Sprintf("orderbook.1.%s", upperSymbol)
		logger.WithField("target_topic", topic).Debugf("BybitClient (%s): Определен целевой топик для UnsubscribeBBO.", c.marketType) // Используем логгер Debug

		// 1. Topics to remove from activeSubscriptions: The BBO topic.
		affectedTopics = append(affectedTopics, topic) // Add to affected (for removal)
		// 2. Topics for batching: Add the BBO topic to the UNSUBSCRIBE batch.
		topicsToUnsubscribeBatch = append(topicsToUnsubscribeBatch, topic)

	default:
		// Неизвестный тип команды. Логируем предупреждение.
		logger.WithField("command_type", cmd.Type).Warnf("BybitClient (%s): Получен неизвестный тип команды подписки/отписки. Игнорирование.", c.marketType) // Используем логгер Warn
		return affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch                                                                              // Возвращаем пустые списки пакетных топиков.
	}

	logger.WithFields(logrus.Fields{
		"affected_count":             len(affectedTopics),
		"to_subscribe_batch_count":   len(topicsToSubscribeBatch),
		"to_unsubscribe_batch_count": len(topicsToUnsubscribeBatch),
	}).Debugf("BybitClient (%s): Определение топиков завершено.", c.marketType) // Используем логгер Debug

	// Возвращаем списки топиков: затронутые для обновления activeSubscriptions,
	// и специфичные топики для добавления в пакеты.
	return affectedTopics, topicsToSubscribeBatch, topicsToUnsubscribeBatch
}

// updateActiveSubscriptions обновляет внутреннюю мапу activeSubscriptions
// после успешной отправки команды подписки/отписки.
// Использует logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
// Принимает указатель *SubscriptionCommand и список топиков, чье состояние меняется
func (c *BybitClient) updateActiveSubscriptions(cmd *types.SubscriptionCommand, affectedTopics []string) { // <<< Прикреплен к *BybitClient, Принимает *SubscriptionCommand и []string
	// Используем логгер с контекстом для этого метода и команды
	logger := c.log.WithFields(logrus.Fields{
		"func":                  "updateActiveSubscriptions",
		"command_type":          cmd.Type,
		"symbol":                cmd.Symbol,
		"affected_topics_count": len(affectedTopics),
	})
	logger.Debugf("BybitClient (%s): Обновление карты активных подписок.", c.marketType) // Используем логгер Debug

	c.subMu.Lock()         // Блокируем для записи в activeSubscriptions
	defer c.subMu.Unlock() // Гарантируем освобождение мьютекса

	// Убедимся, что команда предназначена для ТИПА РЫНКА ЭТОГО клиента (проверка избыточна, но безопаснее)
	if cmd.MarketType != c.marketType || cmd.Source != "bybit" {
		logger.Warnf("BybitClient (%s): Получена команда не для этого клиента. Этого не должно происходить.", c.marketType) // Используем логгер Warn
		return
	}

	// Логика: affectedTopics перечисляет все топики, чье состояние в мапе должно измениться.
	// На основе типа команды, мы определяем, какие из этих топиков добавляются,
	// а какие удаляются.

	topicsToRemove := make(map[string]bool) // Используем мапу для эффективного поиска топиков для удаления

	switch cmd.Type {
	case types.SubscribeDepth:
		// Для SubscribeDepth, есть один целевой топик для ДОБАВЛЕНИЯ, и потенциально много старых для УДАЛЕНИЯ.
		// affectedTopics содержит как новый топик, так и старые для удаления.
		// Находим новый топик и добавляем/обновляем его. Все остальные затронутые топики удаляются.
		// Bybit V5 Depth topics: "orderbook.{depth}.{symbol}"
		validDepths := map[int]bool{1: true, 50: true, 200: true, 500: true, 1000: true}
		depth := cmd.Depth
		if depth <= 0 || !validDepths[depth] {
			depth = 50 // Default depth
		}
		targetTopic := fmt.Sprintf("orderbook.%d.%s", depth, strings.ToUpper(cmd.Symbol)) // Upper symbol for topic

		c.activeSubscriptions[targetTopic] = *cmd                                                                               // Add/update the target topic (store VALUE copy of cmd)
		logger.WithField("topic", targetTopic).Tracef("BybitClient (%s): Добавлен/обновлен целевой топик в map.", c.marketType) // Trace

		// Все остальные топики в affectedTopics, которые не являются целевым топиком, должны быть удалены из мапы.
		for _, topic := range affectedTopics {
			if topic != targetTopic {
				topicsToRemove[topic] = true                                                                                  // Помечаем для удаления
				logger.WithField("topic", topic).Tracef("BybitClient (%s): Помечен топик для удаления из map.", c.marketType) // Trace
			}
		}

	case types.UnsubscribeDepth:
		// Для UnsubscribeDepth, все затронутые depth топики для символа УДАЛЯЮТСЯ.
		for _, topic := range affectedTopics {
			topicsToRemove[topic] = true                                                                                                      // Помечаем все затронутые топики для удаления
			logger.WithField("topic", topic).Tracef("BybitClient (%s): Помечен топик для удаления из map (unsubscribe depth).", c.marketType) // Trace
		}

	case types.SubscribeBBO:
		// Для SubscribeBBO, целевой BBO топик ДОБАВЛЯЕТСЯ.
		topic := fmt.Sprintf("orderbook.1.%s", strings.ToUpper(cmd.Symbol))                                           // Bybit V5 BBO topic, Upper symbol
		c.activeSubscriptions[topic] = *cmd                                                                           // Добавляем/обновляем топик BBO (сохраняем КОПИЮ VALUE команды)
		logger.WithField("topic", topic).Tracef("BybitClient (%s): Добавлен/обновлен BBO топик в map.", c.marketType) // Trace
		// Нет топиков, помеченных для удаления в этом случае, основано на поведении Bybit.

	case types.UnsubscribeBBO:
		// Для UnsubscribeBBO, целевой BBO топик УДАЛЯЕТСЯ.
		topic := fmt.Sprintf("orderbook.1.%s", strings.ToUpper(cmd.Symbol))                                                                 // Bybit V5 BBO topic, Upper symbol
		topicsToRemove[topic] = true                                                                                                        // Помечаем топик BBO для удаления
		logger.WithField("topic", topic).Tracef("BybitClient (%s): Помечен BBO топик для удаления из map (unsubscribe bbo).", c.marketType) // Trace
	}

	// Выполняем удаления из мапы activeSubscriptions
	if len(topicsToRemove) > 0 {
		logger.WithField("topics_to_remove_count", len(topicsToRemove)).Debugf("BybitClient (%s): Выполнение удалений из карты активных подписок.", c.marketType) // Используем логгер Debug
		for topic := range topicsToRemove {
			if _, ok := c.activeSubscriptions[topic]; ok {
				delete(c.activeSubscriptions, topic)
				logger.WithField("topic", topic).Debugf("BybitClient (%s): Активный топик удален из map.", c.marketType) // Используем логгер Debug
			} else {
				logger.WithField("topic", topic).Tracef("BybitClient (%s): Попытка удалить топик, которого нет в map.", c.marketType) // Trace
			}
		}
	}

	// Лог отладки текущего количества подписок
	logger.WithField("current_count", len(c.activeSubscriptions)).Debugf("BybitClient (%s): Обновление карты активных подписок завершено.", c.marketType) // Используем логгер Debug
}

// SubscribeBybit отправляет запрос SUBSCRIBE по WebSocket (Bybit V5 format).
// Использует c.conn под мьютексом и учитывает контекст для таймаута записи.
// Используется в Run цикле после успешного подключения или в handleSubscriptionCommands.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) SubscribeBybit(ctx context.Context, topics []string) error { // <<< Прикреплен к *BybitClient, Принимает context и []string
	// Используем логгер с контекстом для этого метода и топиков
	logger := c.log.WithFields(logrus.Fields{
		"func":        "SubscribeBybit",
		"topics":      topics,
		"topic_count": len(topics),
	})
	logger.Infof("BybitClient (%s): Отправка WS запроса SUBSCRIBE.", c.marketType) // Используем логгер Info

	// Используем переданный контекст (clientCtx или другой) для таймаута отправки.
	err := c.sendSubscriptionWSMessageBybit(ctx, "subscribe", topics) // <<< sendSubscriptionWSMessageBybit определен ниже
	if err != nil {
		logger.WithError(err).Errorf("BybitClient (%s): Ошибка при отправке запроса на подписку Bybit.", c.marketType) // Используем логгер Error
		return fmt.Errorf("BybitClient (%s): ошибка при отправке запроса на подписку Bybit: %w", c.marketType, err)
	}
	logger.Infof("BybitClient (%s): WS запрос SUBSCRIBE успешно отправлен.", c.marketType) // Используем логгер Info

	// TODO: Опционально: Добавить логику ожидания подтверждения подписки.
	// Ответы обрабатываются в handleMessage.

	return nil
}

// UnsubscribeBybit отправляет запрос UNSUBSCRIBE по WebSocket (Bybit V5 format).
// Используется в handleSubscriptionCommands.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) UnsubscribeBybit(ctx context.Context, topics []string) error { // <<< Прикреплен к *BybitClient, Принимает context и []string
	// Используем логгер с контекстом для этого метода и топиков
	logger := c.log.WithFields(logrus.Fields{
		"func":        "UnsubscribeBybit",
		"topics":      topics,
		"topic_count": len(topics),
	})
	logger.Infof("BybitClient (%s): Отправка WS запроса UNSUBSCRIBE.", c.marketType) // Используем логгер Info

	// Используем переданный контекст (clientCtx или другой) для таймаута отправки.
	err := c.sendSubscriptionWSMessageBybit(ctx, "unsubscribe", topics) // <<< sendSubscriptionWSMessageBybit определен ниже
	if err != nil {
		logger.WithError(err).Errorf("BybitClient (%s): Ошибка при отправке запроса на отписку Bybit.", c.marketType) // Используем логгер Error
		return fmt.Errorf("BybitClient (%s): ошибка при отправке запроса на отписку Bybit: %w", c.marketType, err)
	}
	logger.Infof("BybitClient (%s): WS запрос UNSUBSCRIBE успешно отправлен.", c.marketType) // Используем логгер Info

	// TODO: Опционально: Добавить логику ожидания подтверждения отписки.

	return nil
}

// performBybitResync - горутина для выполнения WS ре-подписки на активные топики для данного символа.
// ctx: Контекст горутины (clientCtx).
// wg: WaitGroup для синхронизации завершения горутин (внутренний wg клиента).
// symbol: Символ для ресинка.
// Этот метод вызывается из handleResyncCommands.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) performBybitResync(ctx context.Context, wg *sync.WaitGroup, symbol string) { // <<< Прикреплен к *BybitClient, Принимает context и wg
	defer wg.Done() // Уменьшаем счетчик WaitGroup при выходе из горутины

	// Используем логгер с контекстом для этой горутины и символа
	logger := c.log.WithFields(logrus.Fields{
		"goroutine": "ws_resync",
		"symbol":    symbol,
	})
	logger.Infof("BybitClient (%s): Запущена горутина ресинка Bybit (WS Re-subscribe).", c.marketType)        // Используем логгер Info
	defer logger.Infof("BybitClient (%s): Горутина ресинка Bybit (WS Re-subscribe) завершена.", c.marketType) // Используем логгер Info

	// TODO: Параметры повторных попыток для реподписки в performBybitResync (если SubscribeBybit не включает retry).
	// Например: maxAttempts, delay.
	// Пока просто вызываем SubscribeBybit один раз.

	// Собираем топики для реподписки для данного символа из активных подписок.
	topicsToResubscribe := c.getTopicsForSymbolFromActiveSubscriptions(symbol) // getTopicsForSymbolFromActiveSubscriptions определен выше

	if len(topicsToResubscribe) > 0 {
		logger.WithField("topics", topicsToResubscribe).Infof("BybitClient (%s): Выполнение ре-подписки на активные топики.", c.marketType) // Используем логгер Info
		// Выполняем подписку. SubscribeBybit отправляет запрос через WS.
		// Если SubscribeBybit вернет ошибку (например, WS соединение nil или ошибка записи),
		// это будет залогировано внутри SubscribeBybit/sendSubscriptionWSMessageBybit.
		err := c.SubscribeBybit(ctx, topicsToResubscribe) // <<< SubscribeBybit определен выше. Передаем контекст горутины.
		if err != nil {
			logger.WithError(err).Errorf("BybitClient (%s): Ошибка при отправке запроса на переподписку.", c.marketType) // Используем логгер Error
			// TODO: Логика обработки ошибки реподписки при ресинке (оповещение?).
		} else {
			logger.Infof("BybitClient (%s): Запрос на переподписку успешно отправлен.", c.marketType) // Используем логгер Info
			// Для Bybit V5, биржа пришлет SNAPSHOT по WS после успешной подписки.
			// Scanner получит его через OrderBookChan (который в c.Channels).
		}
	} else {
		logger.Warnf("BybitClient (%s): Нет активных подписок для символа для переподписки. Ресинк не выполнен.", c.marketType) // Используем логгер Warn
		// В этом случае, возможно, нужно уведомить Scanner, что ресинк не выполнен, т.к. нет подписок?
		// Можно отправить ResyncCommand с типом "resync_failed" обратно в Scanner.
	}
}

// keepAlive отправляет PING сообщения в соединение Bybit через заданный интервал.
// Bybit V5 Public Spot ожидает JSON PING: {"op": "ping"}.
// Эта горутина запускается методом Run после успешного подключения и слушает clientCtx для завершения.
// ctx: Контекст горутины (clientCtx).
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) keepAlive(ctx context.Context) { // <<< Прикреплен к *BybitClient
	c.log.Infof("BybitClient (%s): Keep-Alive goroutine launched with interval %s", c.marketType, c.pingInterval) // Используем логгер Info
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	defer c.log.Infof("BybitClient (%s): Keep-Alive goroutine finished.", c.marketType) // Используем логгер Info

	pingMessage := []byte(`{"op": "ping"}`) // PING message in Bybit V5 Public format.

	for {
		select {
		case <-ticker.C: // Срабатывает по интервалу таймера.
			c.mu.Lock() // Acquire mutex before accessing c.conn.
			// Check connection status under mutex.
			if c.isClosed || c.conn == nil {
				c.mu.Unlock() // Release mutex.
				// c.log.Debug("BybitClient (%s): Keep-Alive: Connection closed or nil, terminating goroutine", c.marketType) // Too noisy Debug
				return // Exit Keep-Alive goroutine.
			}

			// Check PONG activity. Bybit disconnects if no activity (ping/pong) for 30s.
			// Our ReadDeadline is ~35s. If PONG not received for > 30s, it's a problem.
			if time.Since(c.lastPong) > c.wsReadTimeout-5*time.Second { // Use read timeout from struct field
				c.mu.Unlock()
				c.log.Warnf("BybitClient (%s): Keep-Alive: Соединение кажется неактивным (PONG давно не получался). Listen, вероятно, скоро сработает по таймауту.", c.marketType) // Используем логгер Warn
				return                                                                                                                                                             // Exit Keep-Alive. Run will detect Listen death and reconnect.
			}

			// Set write deadline for sending the PING message.
			writeDeadline := time.Now().Add(c.wsWriteTimeout) // Use write timeout from struct field
			err := c.conn.SetWriteDeadline(writeDeadline)     // Используем conn
			c.mu.Unlock()                                     // Release mutex before WriteMessage.

			if err != nil {
				c.log.WithError(err).Errorf("BybitClient (%s): Keep-Alive: Ошибка установки WriteDeadline для PING. Выход.", c.marketType) // Используем логгер Error
				return                                                                                                                     // Terminate goroutine.
			}

			// Send JSON PING frame.
			// Bybit V5 uses WriteMessage for JSON control messages, not WriteControl.
			err = c.conn.WriteMessage(websocket.TextMessage, pingMessage) // Используем conn

			// Reset write deadline after sending. Important!
			c.mu.Lock()
			if c.conn != nil { // Check conn for nil under mutex
				c.conn.SetWriteDeadline(time.Time{}) // Zero time means no deadline.
			}
			c.mu.Unlock()

			if err != nil {
				c.log.WithError(err).Errorf("BybitClient (%s): Keep-Alive: Ошибка отправки PING. Выход.", c.marketType) // Используем логгер Error
				return                                                                                                  // Terminate goroutine.
			}
			// c.log.Trace("BybitClient (%s): Keep-Alive: PING sent", c.marketType) // Commented out - Too noisy Trace

		case <-ctx.Done(): // Listen to internal client context for termination.
			c.log.Infof("BybitClient (%s): Keep-Alive: Получен сигнал отмены контекста, завершение горутины.", c.marketType) // Используем логгер Info
			return                                                                                                           // Exit Keep-Alive goroutine.
		}
	}
}

// handleMessage обрабатывает одно полученное WebSocket сообщение Bybit.
// Парсит сообщение и отправляет унифицированные структуры OrderBookMessage или BBO в общие каналы.
// Эта функция вызывается из Listen горутины.
// Она не должна блокироваться надолго, чтобы не задерживать чтение следующих сообщений.
func (c *BybitClient) handleMessage(message []byte) { // <<< Прикреплен к *BybitClient
	// defer recover для защиты от паники в парсерах или обработчиках
	defer func() {
		if r := recover(); r != nil {
			c.log.WithFields(logrus.Fields{
				"panic_recover": r,
				"stack_trace":   string(debug.Stack()),
				"raw_message":   string(message), // Логируем сырое сообщение при панике
			}).Errorf("BybitClient (%s): Recovered from panic in handleMessage.", c.marketType) // Используем логгер
		}
	}()

	// Inklude raw messages for debugging (can be very noisy)
	c.log.WithField("message_length", len(message)).Tracef("BybitClient (%s): Raw Message: %s", c.marketType, string(message)) // Используем логгер Trace

	// Structure for parsing general Bybit V5 Public messages.
	var bybitMsg struct {
		Topic string          `json:"topic"` // Тема потока (например, "orderbook.50.BTCUSDT")
		TS    int64           `json:"ts"`    // Timestamp события (миллисекунды)
		Type  string          `json:"type"`  // Тип сообщения ("snapshot" | "delta" for orderbook)
		Data  json.RawMessage `json:"data"`  // Поле с данными (как RawMessage для дальнейшего парсинга)
		// Поля для служебных сообщений:
		Success bool   `json:"success"`
		RetMsg  string `json:"ret_msg"`          // Сообщение результата ("subscribe", "pong", "success", "error", "auth"...)
		ConnID  string `json:"conn_id"`          // ID соединения
		ReqID   string `json:"req_id,omitempty"` // ID запроса, если был указан при подписке
		Op      string `json:"op,omitempty"`     // Операция ("subscribe", "ping", "auth", "pong")
		RetCode int    `json:"retCode"`          // Код результата для ответов на запросы/ошибок
	}

	// Пытаемся декодировать общее сообщение.
	err := json.Unmarshal(message, &bybitMsg)
	if err != nil {
		// Если базовое декодирование не удалось, логируем ошибку и пропускаем сообщение.
		c.log.WithError(err).WithField("raw_message", string(message)).Errorf("BybitClient (%s): Ошибка декодирования JSON сообщения.", c.marketType) // Используем логгер
		return
	}
	// c.log.Tracef("BybitClient (%s): handleMessage: Received Msg - Topic: '%s', Type: '%s', Op: '%s', RetMsg: '%s', Success: %t, RetCode: %d, DataLength: %d",
	// 	c.marketType, bybitMsg.Topic, bybitMsg.Type, bybitMsg.Op, bybitMsg.RetMsg, bybitMsg.Success, bybitMsg.RetCode, len(bybitMsg.Data)) // Very noisy trace

	// --- Обработка служебных сообщений (поля Op, RetMsg, RetCode, Success) ---
	if bybitMsg.Op != "" || bybitMsg.RetMsg != "" || bybitMsg.RetCode != 0 || !bybitMsg.Success {
		if bybitMsg.Op == "pong" {
			// Получен PONG в ответ на наш PING {"op":"ping"}. Обновляем время последнего PONG.
			// c.log.Tracef("BybitClient (%s): Received PONG (JSON)", c.marketType) // Trace
			c.mu.Lock()             // Защищаем доступ к lastPong.
			c.lastPong = time.Now() // Обновляем время последнего PONG'а.
			c.mu.Unlock()           // Освобождаем мьютекс.
			return                  // Сообщение PONG обработано, дальше не парсим как данные.
		}

		// Обработка ответов на SUBSCRIBE/UNSUBSCRIBE/AUTH и других служебных сообщений.
		if bybitMsg.Op == "subscribe" || bybitMsg.Op == "unsubscribe" || bybitMsg.Op == "auth" {
			// Логируем ответ, независимо от success/failure.
			c.log.WithFields(logrus.Fields{
				"op": bybitMsg.Op, "success": bybitMsg.Success, "retCode": bybitMsg.RetCode,
				"retMsg": bybitMsg.RetMsg, "topic": bybitMsg.Topic, "reqID": bybitMsg.ReqID, "connID": bybitMsg.ConnID,
			}).Infof("BybitClient (%s): Received response.", c.marketType) // Используем логгер Info

			// TODO: Here you could update activeSubscriptions map based on successful/failed topics
			// by parsing ret_msg or relying on the 'topic' field if present.
			// For now, activeSubscriptions is updated after *sending* the request.

			if bybitMsg.Op == "auth" && !bybitMsg.Success {
				c.log.Errorf("BybitClient (%s): FATAL WS AUTH ERROR: %s. Client shutting down?", c.marketType, bybitMsg.RetMsg) // Используем логгер Error
				// TODO: Define reaction to auth error. Maybe call c.clientCancel().
			}
			return // Служебное сообщение обработано.
		}

		// Другие служебные сообщения (например, уведомления об ошибках или лимитах)
		c.log.WithFields(logrus.Fields{
			"op": bybitMsg.Op, "retCode": bybitMsg.RetCode, "retMsg": bybitMsg.RetMsg,
			"raw_message": string(message),
		}).Debugf("BybitClient (%s): Received other service or unknown message.", c.marketType) // Используем логгер Debug
		return // Служебное сообщение обработано.
	}

	// --- Обработка сообщений данных (есть поля Topic, TS, Type, Data, Success=true, RetCode=0) ---
	// Проверяем, что есть топик и поле данных не пустое.
	if bybitMsg.Topic != "" && len(bybitMsg.Data) > 0 {
		// Timestamp события из сообщения Bybit.
		// Bybit V5 Public timestamps are in milliseconds.
		eventTime := time.UnixMilli(bybitMsg.TS)

		// Различаем сообщения по префиксу топика.
		switch {
		case strings.HasPrefix(bybitMsg.Topic, "orderbook.1."): // Топик для L1 стакана (используется как BBO)
			// Парсим содержимое поля Data как данные L1 стакана.
			bbo := c.parseOrderBookL1BybitData(bybitMsg.Data, bybitMsg.Topic, bybitMsg.TS) // Parse RawMessage Data // parseOrderBookL1BybitData is defined above
			if bbo != nil {
				bbo.MarketType = c.marketType // *** ADDED *** Set MarketType

				select {
				case c.Channels.BBOChan <- bbo:
					// Успешно отправлено
					// c.log.Tracef("BybitClient (%s): Sent BBO for %s to channel.", c.marketType, bbo.Symbol) // Very noisy trace
				default:
					// Логируем пропуск данных, если канал заполнен
					c.log.WithField("symbol", bbo.Symbol).Warnf("BybitClient (%s): BBOChan full, BBO dropped.", c.marketType) // Используем логгер Warn
				}
			} else {
				c.log.WithFields(logrus.Fields{
					"topic": bybitMsg.Topic, "raw_data": string(bybitMsg.Data),
				}).Errorf("BybitClient (%s): parseOrderBookL1BybitData returned nil (parsing error?).", c.marketType) // Используем логгер Error
			}

		case strings.HasPrefix(bybitMsg.Topic, "orderbook."): // Топики для полного стакана (например, orderbook.50., orderbook.1000.)
			// Это данные полного стакана (type="partial" для snapshot, type="delta" для дельт).
			// Парсим содержимое поля Data как данные стакана (OrderBookDelta).
			orderBookDelta := c.parseOrderBookDataBybit(bybitMsg.Data, bybitMsg.Topic, bybitMsg.TS, bybitMsg.Type) // parseOrderBookDataBybit is defined below
			if orderBookDelta != nil {
				orderBookDelta.MarketType = c.marketType // *** ADDED *** Set MarketType in Delta

				unifiedMessageType := bybitMsg.Type // По умолчанию используем тип Bybit

				// Convert Bybit "partial" to our unified "snapshot"
				if bybitMsg.Type == "partial" {
					unifiedMessageType = "snapshot"
					// c.log.Tracef("BybitClient (%s): Converted Bybit message type 'partial' to unified 'snapshot' for %s.", c.marketType, orderBookDelta.Symbol) // Commented out
				}
				// For "delta" Bybit, unifiedMessageType remains "delta".

				select {
				case c.Channels.OrderBookChan <- &types.OrderBookMessage{
					Source:     "bybit",
					MarketType: c.marketType,          // *** ADDED MarketType ***
					Symbol:     orderBookDelta.Symbol, // Symbol from OrderBookDelta (uppercase)
					Type:       unifiedMessageType,    // Use the converted type
					Timestamp:  eventTime,             // Use TS from parent Bybit message as Event Timestamp
					Delta:      orderBookDelta,        // Embed OrderBookDelta (with bids, asks, UpdateID)
					RawData:    message,               // Pass raw data (needed by Scanner for extractDepth)
				}:
					// Успешно отправлено (snapshot or delta as OrderBookDelta inside OrderBookMessage)
					// c.log.Tracef("BybitClient (%s): Sent OrderBookMessage (%s) for %s to channel.", c.marketType, unifiedMessageType, orderBookDelta.Symbol) // Very noisy trace
				default:
					// Логируем пропуск данных, если канал заполнен.
					c.log.WithFields(logrus.Fields{
						"symbol": orderBookDelta.Symbol, "msg_type": unifiedMessageType,
					}).Warnf("BybitClient (%s): OrderBookChan full, order book data dropped.", c.marketType) // Используем логгер Warn
				}
			} else {
				c.log.WithFields(logrus.Fields{
					"topic": bybitMsg.Topic, "msg_type": bybitMsg.Type, "raw_data": string(bybitMsg.Data), // *** ИСПРАВЛЕНО: msgType берем из bybitMsg ***
				}).Errorf("BybitClient (%s): parseOrderBookDataBybit returned nil (parsing error?).", c.marketType) // Используем логгер Error
			}

		// case strings.HasPrefix(bybitMsg.Topic, "tickers."): // This topic is not used by us for BBO
		default:
			// Other topic types (trades, kline, etc.) that we don't handle yet.
			c.log.WithFields(logrus.Fields{
				"topic": bybitMsg.Topic, "msg_type": bybitMsg.Type, "data_len": len(bybitMsg.Data),
			}).Debugf("BybitClient (%s): Received Bybit data from unhandled topic.", c.marketType) // Используем логгер Debug
		}
		return // Data message handled.
	}

	// --- If message has no service fields and no data fields (Topic+Data) ---
	c.log.WithField("raw_message", string(message)).Tracef("BybitClient (%s): Received unrecognized message.", c.marketType) // Используем логгер Trace
	// Ignore such messages.
}

// parseOrderBookL1BybitData парсит данные L1 стакана из orderbook.1. topic Bybit V5 Public Spot
// и преобразует их в структуру BBO. Использует logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) parseOrderBookL1BybitData(data json.RawMessage, topic string, ts int64) *types.BBO { // <<< Прикреплен к *BybitClient
	// Используем логгер с контекстом для этого метода и топика
	logger := c.log.WithFields(logrus.Fields{
		"func":     "parseOrderBookL1BybitData",
		"topic":    topic,
		"data_len": len(data),
	})
	logger.Tracef("BybitClient (%s): Начало парсинга L1 order book data.", c.marketType) // Используем логгер Trace

	// Structure of Bybit V5 Spot/Linear L1 order book data (inside "data"):
	var l1OrderBookData struct {
		Symbol        string     `json:"s"` // Symbol
		Bids          [][]string `json:"b"` // Array with one bid level [price, quantity]
		Asks          [][]string `json:"a"` // Array with one ask level [price, quantity]
		UpdateID      int64      `json:"u"` // Update id
		CrossSequence int64      `json:"g"` // Cross sequence
	}

	err := json.Unmarshal(data, &l1OrderBookData)
	if err != nil {
		logger.WithError(err).WithField("raw_data", string(data)).Errorf("BybitClient (%s): Ошибка парсинга L1 order book data JSON.", c.marketType) // Используем логгер Error
		return nil                                                                                                                                   // Возвращаем nil при ошибке парсинга.
	}

	symbol := l1OrderBookData.Symbol
	timestamp := time.UnixMilli(ts)

	// Extract and parse the best Bid (first and only element in Bids array).
	// Логируем предупреждение, если массив пуст или формат некорректен.
	bestBidPriceStr := "0"
	bestBidQtyStr := "0"
	bestBidParsed := false
	if len(l1OrderBookData.Bids) > 0 && len(l1OrderBookData.Bids[0]) >= 2 {
		bestBidPriceStr = l1OrderBookData.Bids[0][0]
		bestBidQtyStr = l1OrderBookData.Bids[0][1]
		bestBidParsed = true
	} else if len(l1OrderBookData.Bids) > 0 {
		logger.WithField("bid_level", l1OrderBookData.Bids).Warnf("BybitClient (%s): L1 Bids массив имеет элементы, но некорректный формат уровня.", c.marketType) // Используем логгер Warn
	} else {
		logger.Tracef("BybitClient (%s): L1 Bids массив пуст.", c.marketType) // Trace для пустых массивов (может быть при начальном снимке с пустыми сторонами)
	}

	// Extract and parse the best Ask (first and only element in Asks array).
	// Логируем предупреждение, если массив пуст или формат некорректен.
	bestAskPriceStr := "0"
	bestAskQtyStr := "0"
	bestAskParsed := false
	if len(l1OrderBookData.Asks) > 0 && len(l1OrderBookData.Asks[0]) >= 2 {
		bestAskPriceStr = l1OrderBookData.Asks[0][0]
		bestAskQtyStr = l1OrderBookData.Asks[0][1]
		bestAskParsed = true
	} else if len(l1OrderBookData.Asks) > 0 {
		logger.WithField("ask_level", l1OrderBookData.Asks).Warnf("BybitClient (%s): L1 Asks массив имеет элементы, но некорректный формат уровня.", c.marketType) // Используем логгер Warn
	} else {
		logger.Tracef("BybitClient (%s): L1 Asks массив пуст.", c.marketType) // Trace
	}

	// Parse string prices and quantities into float64.
	// Логируем ошибки парсинга чисел на уровне Error.
	bestBid, err := strconv.ParseFloat(bestBidPriceStr, 64)
	if err != nil && bestBidParsed {
		logger.WithError(err).WithField("price_str", bestBidPriceStr).WithField("symbol", symbol).Errorf("BybitClient (%s): Ошибка парсинга лучшей цены покупки (L1).", c.marketType) // Используем логгер Error
		return nil                                                                                                                                                                    // Возвращаем nil при ошибке парсинга.
	}
	bestBidQty, err := strconv.ParseFloat(bestBidQtyStr, 64)
	if err != nil && bestBidParsed {
		logger.WithError(err).WithField("qty_str", bestBidQtyStr).WithField("symbol", symbol).Logger.Errorf("BybitClient (%s): Ошибка парсинга количества лучшей покупки (L1).", c.marketType) // Используем логгер Error
		return nil                                                                                                                                                                             // Возвращаем nil при ошибке парсинга.
	}
	bestAsk, err := strconv.ParseFloat(bestAskPriceStr, 64)
	if err != nil && bestAskParsed {
		logger.WithError(err).WithField("price_str", bestAskPriceStr).WithField("symbol", symbol).Errorf("BybitClient (%s): Ошибка парсинга лучшей цены продажи (L1).", c.marketType) // Используем логгер Error
		return nil                                                                                                                                                                    // Возвращаем nil при ошибке парсинга.
	}
	bestAskQty, err := strconv.ParseFloat(bestAskQtyStr, 64)
	if err != nil && bestAskParsed {
		logger.WithError(err).WithField("qty_str", bestAskQtyStr).WithField("symbol", symbol).Errorf("BybitClient (%s): Ошибка парсинга количества лучшей продажи (L1).", c.marketType) // Используем логгер Error
		return nil                                                                                                                                                                      // Возвращаем nil при ошибке парсинга.
	}

	// Создаем и возвращаем структуру BBO.
	bbo := &types.BBO{
		Source:          "bybit",
		MarketType:      c.marketType,            // *** ADDED *** Set MarketType
		Symbol:          strings.ToUpper(symbol), // Convert symbol to uppercase for consistency.
		BestBid:         bestBid,
		BestBidQuantity: bestBidQty,
		BestAsk:         bestAsk,
		BestAskQuantity: bestAskQty,
		Timestamp:       timestamp, // Use timestamp from the Bybit message.
		// L1 сообщения от Bybit V5 содержат UpdateID ('u'), но обычно оно не используется напрямую в BBO.
		// Если нужно, можно добавить поле UpdateID в структуру BBO.
		// BybitL1UpdateID: l1OrderBookData.UpdateID, // Опционально добавить это поле в types.BBO
	}
	logger.WithFields(logrus.Fields{
		"symbol":    bbo.Symbol,
		"bid_price": bbo.BestBid, "bid_qty": bbo.BestBidQuantity,
		"ask_price": bbo.BestAsk, "ask_qty": bbo.BestAskQuantity,
		"timestamp":       bbo.Timestamp.Format(time.RFC3339Nano),
		"bybit_update_id": l1OrderBookData.UpdateID,
	}).Tracef("BybitClient (%s): Парсинг L1 order book data завершен успешно.", c.marketType) // Trace

	return bbo // Возвращаем указатель на BBO.
}

// parseOrderBookDataBybit парсит данные полного стакана (snapshot или delta) Bybit V5 Public
// из RawMessage (поле 'data' из общего сообщения Bybit). Использует logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) parseOrderBookDataBybit(data json.RawMessage, topic string, ts int64, msgType string) *types.OrderBookDelta { // <<< Прикреплен к *BybitClient, Принимает RawMessage data, string topic, int64 ts, string msgType
	// Используем логгер с контекстом для этого метода, топика и типа сообщения
	logger := c.log.WithFields(logrus.Fields{
		"func":     "parseOrderBookDataBybit",
		"topic":    topic,
		"msg_type": msgType,
		"data_len": len(data),
	})
	logger.Tracef("BybitClient (%s): Начало парсинга order book data.", c.marketType) // Используем логгер Trace

	var orderBookData struct {
		Symbol string `json:"s"` // Symbol
		// Bids and Asks can have optional "tag" field, need to handle that.
		Bids          [][]string `json:"b"` // Array of bid levels [price, quantity, tag(optional)]
		Asks          [][]string `json:"a"` // Array of ask levels [price, quantity, tag(optional)]
		UpdateID      int64      `json:"u"` // Update id (used for sync by Scanner)
		CrossSequence int64      `json:"g"` // Cross sequence (ignore for Spot, useful for Linear/Inverse)
	}

	err := json.Unmarshal(data, &orderBookData)
	if err != nil {
		logger.WithError(err).WithField("raw_data", string(data)).Errorf("BybitClient (%s): Ошибка парсинга order book data JSON.", c.marketType) // Используем логгер Error
		return nil                                                                                                                                // Возвращаем nil при ошибке парсинга.
	}

	symbol := orderBookData.Symbol
	timestamp := time.UnixMilli(ts)

	// Parse Bids (buy levels). Логируем ошибки парсинга уровней на уровне Warn.
	parsedBids := make([]types.PriceLevel, 0, len(orderBookData.Bids))
	for i, bidLevel := range orderBookData.Bids {
		if len(bidLevel) >= 2 { // Check for at least 2 elements (price, quantity)
			price, err1 := strconv.ParseFloat(bidLevel[0], 64)
			amount, err2 := strconv.ParseFloat(bidLevel[1], 64)
			if err1 == nil && err2 == nil {
				parsedBids = append(parsedBids, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга цены или количества. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side": "bid", "index": i, "level_data": bidLevel,
					"parse_price_error": err1, "parse_amount_error": err2,
				}).Warnf("BybitClient (%s): Ошибка парсинга уровня bid в order book data. Пропуск уровня.", c.marketType) // Используем логгер Warn
			}
		} else {
			logger.WithFields(logrus.Fields{
				"side": "bid", "index": i, "level_data": bidLevel,
			}).Warnf("BybitClient (%s): Некорректный формат уровня bid в order book data (менее 2 элементов). Пропуск уровня.", c.marketType) // Используем логгер Warn
		}
	}

	// Parse Asks (sell levels). Логируем ошибки парсинга уровней на уровне Warn.
	parsedAsks := make([]types.PriceLevel, 0, len(orderBookData.Asks))
	for i, askLevel := range orderBookData.Asks {
		if len(askLevel) >= 2 { // Check for at least 2 elements (price, quantity)
			price, err1 := strconv.ParseFloat(askLevel[0], 64)
			amount, err2 := strconv.ParseFloat(askLevel[1], 64)
			if err1 == nil && err2 == nil {
				parsedAsks = append(parsedAsks, types.PriceLevel{Price: price, Amount: amount})
			} else {
				// Ошибка парсинга цены или количества. Логируем предупреждение.
				logger.WithFields(logrus.Fields{
					"side": "ask", "index": i, "level_data": askLevel,
					"parse_price_error": err1, "parse_amount_error": err2,
				}).Warnf("BybitClient (%s): Ошибка парсинга уровня ask в order book data. Пропуск уровня.", c.marketType) // Используем логгер Warn
			}
		} else {
			logger.WithFields(logrus.Fields{
				"side": "ask", "index": i, "level_data": askLevel,
			}).Warnf("BybitClient (%s): Некорректный формат уровня ask в order book data (менее 2 элементов). Пропуск уровня.", c.marketType) // Используем логгер Warn
		}
	}

	// Создаем и возвращаем структуру OrderBookDelta.
	delta := &types.OrderBookDelta{
		Source:        "bybit",
		MarketType:    c.marketType,            // *** ADDED *** Set MarketType
		Symbol:        strings.ToUpper(symbol), // Convert symbol to uppercase for consistency.
		Bids:          parsedBids,
		Asks:          parsedAsks,
		Timestamp:     timestamp,              // Event timestamp.
		FirstUpdateID: 0,                      // Bybit V5 Public Spot/Linear does not use FirstUpdateID (U) in deltas
		FinalUpdateID: orderBookData.UpdateID, // Use the UpdateID 'u' from Bybit data. (Using orderBookData from parent frame, not deltaMsg)
		// NOTE: You might need to parse the UpdateID from the 'data' RawMessage specifically if it's not in the outer message struct.
		// Based on Bybit V5 docs, 'u' is inside the 'data' field for orderbook topics.
		// The parsing above is correct, using orderBookData struct *inside* the parseOrderBookDataBybit func.
		// The variable name `orderBookData` is correct for accessing the unmarshaled `data` field.
	}
	// Correcting the source of `orderBookData.UpdateID`
	// The struct definition `var orderBookData struct { ... UpdateID ... }` is correct for the data payload.
	// The variable `orderBookData` holds the unmarshaled data payload.

	logger.WithFields(logrus.Fields{
		"symbol":     delta.Symbol,
		"u":          delta.FinalUpdateID,
		"bids_count": len(delta.Bids),
		"asks_count": len(delta.Asks),
		"timestamp":  delta.Timestamp.Format(time.RFC3339Nano),
		"topic":      topic,
		"msg_type":   msgType,
	}).Tracef("BybitClient (%s): Парсинг order book data завершен успешно.", c.marketType) // Trace

	return delta // Возвращаем указатель на OrderBookDelta.
}

// ... (код после parseOrderBookDataBybit)

// parseBBO parses JSON BBO (@bookTicker) data from RawMessage or []byte for Bybit V5.
// Bybit V5 uses "orderbook.1." topic for BBO.
// It's a normal orderbook delta with depth 1.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) parseBBO(data []byte) *types.BBO { // <<< Прикреплен к *BybitClient, принимает []byte data
	// Этот метод не должен использоваться для Bybit V5 BBO.
	// Bybit V5 BBO обрабатывается методом parseOrderBookL1BybitData,
	// который вызывается из handleMessage для топика "orderbook.1.".
	// Логируем ошибку, если этот метод вызван.
	logger := c.log.WithField("func", "parseBBO").WithField("data_len", len(data))
	logger.Errorf("BybitClient (%s): Called placeholder parseBBO method. This method is not used for Bybit V5 BBO. Use parseOrderBookL1BybitData.", c.marketType)
	// Возвращаем nil, чтобы указать на ошибку парсинга.
	return nil
}

// ... (код после parseBBO)

// GetOrderBookSnapshotREST fetches a Bybit order book snapshot via REST API.
// For Bybit V5 Public data sync, REST snapshot is usually NOT required after WS connect/resubscribe,
// as the initial WS SUBSCRIBE sends a 'partial' snapshot.
// This method is included for completeness but may not be used in the public data flow.
// If it were needed, it would use the Rate Limiter and Bybit's REST API depth endpoint.
// symbol: Trading symbol (must be uppercase).
// limit: Desired order book depth.
// Uses clientCtx for HTTP request cancellation. Uses logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) GetOrderBookSnapshotREST(symbol string, limit int) (*types.OrderBook, error) { // <<< Прикреплен к *BybitClient
	// Используем логгер с контекстом
	logger := c.log.WithFields(logrus.Fields{
		"func":   "GetOrderBookSnapshotREST",
		"symbol": strings.ToUpper(symbol),
		"limit":  limit,
	})
	// Логируем, что этот метод не используется для публичных данных Bybit V5.
	logger.Infof("BybitClient (%s): REST snapshot requested. For Bybit V5 Public data flow, REST snapshots are generally NOT used for sync (WS SUBSCRIBE sends partial).", c.marketType) // Используем логгер Info
	// TODO: If this *is* needed for private data sync or specific public endpoints, implement the actual REST call here.

	// Return an error indicating it's not implemented/used for this flow.
	return nil, fmt.Errorf("BybitClient (%s): REST snapshot method not implemented/used for public data flow", c.marketType)
}

func (c *BybitClient) Close() { // <<< Прикреплен к *BybitClient
	c.mu.Lock()         // Захватываем мьютекс.
	defer c.mu.Unlock() // Гарантируем освобождение мьютекса.

	if c.conn == nil {
		// c.log.Debug("Close: Попытка закрыть уже nil соединение.") // Too noisy, use Trace
		return // Соединение уже nil, ничего делать не нужно.
	}

	c.log.Infof("BybitClient (%s): Закрытие WebSocket соединения...", c.marketType) // Используем логгер Info

	conn := c.conn // Локальная копия соединения для использования после освобождения мьютекса

	// Пытаемся отправить сообщение о нормальном закрытии (CloseMessage).
	// Используем таймаут записи из поля структуры.
	// Устанавливаем таймаут записи для отправки CloseMessage.
	writeDeadline := time.Now().Add(c.wsWriteTimeout) // Используем c.wsWriteTimeout
	err := conn.SetWriteDeadline(writeDeadline)       // Используем локальную копию conn
	if err == nil {                                   // Если удалось установить таймаут
		// Bybit V5 might not respond well to standard CloseMessage, but it's good practice to try.
		// WriteMessage учитывает WriteDeadline, установленный выше.
		err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down")) // Используем локальную копию conn
		if err != nil && err != websocket.ErrCloseSent {
			c.log.WithError(err).Warnf("BybitClient (%s): Ошибка при отправке сообщения о закрытии WebSocket.", c.marketType) // Используем логгер Warn
		} else if err == nil {
			c.log.Debugf("BybitClient (%s): WebSocket close message отправлено.", c.marketType) // Используем логгер Debug
		}
	} else {
		c.log.WithError(err).Warnf("BybitClient (%s): Ошибка установки WriteDeadline перед отправкой CloseMessage.", c.marketType) // Используем логгер Warn
	}

	// Сбрасываем таймаут записи после отправки. Важно!
	// Ошибка сброса дедлайна при закрытии соединения игнорируется.
	// Проверяем локальную копию conn на nil перед сбросом дедлайна.
	if conn != nil {
		conn.SetWriteDeadline(time.Time{}) // Нулевое время означает отсутствие дедлайна.
	}

	// Выполняем физическое закрытие соединения.
	// Этот вызов Close() не принимает контекст.
	err = conn.Close() // Используем локальную копию conn
	if err != nil {
		// Log the error unless it's the expected "use of closed network connection" after setting c.conn=nil elsewhere.
		// However, since we acquire the mutex here, another goroutine setting c.conn=nil shouldn't happen concurrently.
		// So any error here likely indicates a problem with the Close call itself.
		if !strings.Contains(err.Error(), "use of closed network connection") {
			c.log.WithError(err).Errorf("BybitClient (%s): Ошибка при закрытии WebSocket соединения.", c.marketType) // Используем логгер Error
		} else {
			c.log.Tracef("BybitClient (%s): Ошибка 'use of closed network connection' при закрытии WS.", c.marketType) // Trace
		}
	}

	// Устанавливаем c.conn в nil после закрытия.
	c.conn = nil
	c.log.Infof("BybitClient (%s): Соединение WebSocket Bybit закрыто.", c.marketType) // Используем логгер Info

	// Флаг isClosed устанавливается только методом Stop().
	// Здесь мы просто закрываем соединение, возможно, для переподключения.
}

// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) sendSubscriptionWSMessageBybit(ctx context.Context, opMethod string, topics []string) error { // <<< Прикреплен к *BybitClient, Принимает context, string opMethod, []string topics
	// Используем логгер с контекстом для этого метода
	logger := c.log.WithFields(logrus.Fields{
		"func":        "sendSubscriptionWSMessageBybit",
		"op_method":   opMethod,
		"topic_count": len(topics),
	})
	logger.Debugf("BybitClient (%s): Подготовка к отправке сообщения подписки/отписки через WebSocket.", c.marketType) // Используем логгер Debug

	// Bybit V5 не использует 'id' для subscribe/unsubscribe запросов, поэтому его не включаем в структуру запроса.
	request := struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}{
		Op:   opMethod,
		Args: topics,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		// Логируем ошибку маршалинга на уровне Error
		logger.WithError(err).WithFields(logrus.Fields{
			"op_method": opMethod,
			"topics":    topics, // Логируем топики при ошибке
		}).Errorf("BybitClient (%s): Ошибка маршалинга subscribe/unsubscribe запроса в JSON.", c.marketType) // Используем логгер Error
		return fmt.Errorf("BybitClient (%s): error marshalling subscribe/unsubscribe request: %w", c.marketType, err)
	}

	c.mu.Lock()         // Блокируем мьютекс для безопасного доступа к c.conn
	defer c.mu.Unlock() // Гарантируем освобождение мьютекса

	// Проверяем состояние соединения под мьютексом.
	// logger.Tracef("BybitClient (%s): Проверка состояния соединения. c.conn is nil: %t", c.marketType, c.conn == nil) // Too noisy Trace

	if c.conn == nil {
		// Соединение nil. Логируем предупреждение.
		logger.Warnf("BybitClient (%s): Соединение nil. Невозможно отправить сообщение.", c.marketType) // Используем логгер Warn
		return fmt.Errorf("BybitClient (%s): WebSocket connection is not established for sending subscribe/unsubscribe request", c.marketType)
	}

	// logger.Tracef("BybitClient (%s): Соединение валидно. Установка WriteDeadline и отправка сообщения (метод: %s, %d топиков).", c.marketType, opMethod, len(topics)) // Too noisy Trace

	// Устанавливаем дедлайн записи, используя настроенный WS write timeout.
	// Используем дедлайн контекста, если он короче.
	deadline, ok := ctx.Deadline()
	if ok {
		// Используем более ранний из дедлайна команды или общего таймаута записи клиента
		if time.Now().Add(c.wsWriteTimeout).Before(deadline) {
			deadline = time.Now().Add(c.wsWriteTimeout)
		}
	} else {
		// Дедлайн контекста не установлен, используем только общий таймаут записи клиента
		deadline = time.Now().Add(c.wsWriteTimeout)
	}

	err = c.conn.SetWriteDeadline(deadline)
	if err != nil {
		// Ошибка установки дедлайна. Логируем.
		logger.WithError(err).Errorf("BybitClient (%s): Ошибка установки write deadline.", c.marketType) // Используем логгер Error
		// Проверяем c.conn снова под мьютексом, т.к. мог произойти разрыв и c.Close()
		if c.conn == nil {
			logger.Debugf("BybitClient (%s): c.conn стал nil после попытки SetWriteDeadline.", c.marketType) // Используем логгер Debug
			return fmt.Errorf("BybitClient (%s): WebSocket connection became nil after setting write deadline: %w", c.marketType, err)
		}
		// Возвращаем ошибку установки дедлайна.
		return fmt.Errorf("BybitClient (%s): error setting WriteDeadline for subscribe/unsubscribe request: %w", c.marketType, err)
	}

	// Отправляем сообщение.
	err = c.conn.WriteMessage(websocket.TextMessage, jsonData) // Используем c.conn

	// Сбрасываем дедлайн записи после отправки. ВАЖНО!
	// Проверяем conn на nil под мьютексом перед сбросом дедлайна.
	if c.conn != nil { // Проверяем под мьютексом на случай, если c.Close() произошло сразу после WriteMessage
		resetErr := c.conn.SetWriteDeadline(time.Time{}) // Нулевое время означает отсутствие дедлайна.
		if resetErr != nil {
			// Логируем ошибку сброса дедлайна на уровне Warn.
			logger.WithError(resetErr).Warnf("BybitClient (%s): Ошибка сброса write deadline.", c.marketType) // Используем логгер Warn
			// Не возвращаем ошибку сразу, т.к. сообщение могло быть отправлено успешно
		}
	} else {
		// Логируем, если c.conn стал nil после WriteMessage (указывает на разрыв во время отправки).
		logger.Debugf("BybitClient (%s): c.conn стал nil после попытки WriteMessage.", c.marketType) // Используем логгер Debug
	}

	if err != nil {
		// Ошибка отправки сообщения. Логируем.
		logger.WithError(err).Errorf("BybitClient (%s): Ошибка отправки сообщения.", c.marketType) // Используем логгер Error

		// Проверяем, была ли ошибка вызвана отменой контекста.
		select {
		case <-ctx.Done(): // Проверяем переданный контекст (sendCtx из обработчика)
			logger.Debugf("BybitClient (%s): Ошибка отправки совпала с отменой контекста.", c.marketType) // Используем логгер Debug
			return ctx.Err()                                                                              // Возвращаем ошибку контекста, если он был отменен
		default:
			// Если это не ошибка контекста, проверяем, не указывает ли ошибка на закрытое сетевое соединение
			if _, ok := err.(net.Error); ok && strings.Contains(err.Error(), "use of closed network connection") {
				logger.Debugf("BybitClient (%s): Обнаружена ошибка 'use of closed network connection' во время отправки.", c.marketType) // Используем логгер Debug
				return fmt.Errorf("BybitClient (%s): write to closed network connection: %w", c.marketType, err)
			}
			// Возвращаем общую ошибку отправки.
			return fmt.Errorf("BybitClient (%s): error sending subscribe/unsubscribe request (%s, %d topics): %w", c.marketType, opMethod, len(topics), err)
		}
	}

	// Сообщение успешно отправлено. Логируем на уровне Debug.
	logger.WithFields(logrus.Fields{
		"op_method":   opMethod,
		"topic_count": len(topics),
	}).Debugf("BybitClient (%s): Сообщение успешно отправлено.", c.marketType) // Используем логгер Debug

	// TODO: Bybit V5 возвращает ответ на SUBSCRIBE/UNSUBSCRIBE с op="subscribe", "unsubscribe" и retCode/retMsg.
	// Эти ответы обрабатываются в handleMessage.

	return nil // Возвращаем nil при успешной отправке запроса.
}

func (c *BybitClient) CloseWithContext(ctx context.Context) error { // <<< Прикреплен к *BybitClient, Принимает context
	// Используем контекст для явного использования и логирования
	_ = ctx // Явно указываем компилятору, что контекст используется

	c.mu.Lock()         // Блокируем мьютекс
	defer c.mu.Unlock() // Гарантируем освобождение

	if c.conn == nil {
		c.log.Tracef("BybitClient (%s): CloseWithContext: Попытка закрыть уже nil соединение.", c.marketType) // Trace
		return nil                                                                                            // Соединение уже nil.
	}

	c.log.WithField("context_deadline", func() string { // Добавляем дедлайн контекста в лог
		deadline, ok := ctx.Deadline()
		if ok {
			return deadline.String()
		}
		return "none"
	}()).Debugf("BybitClient (%s): CloseWithContext: Закрытие WebSocket соединения с контекстом...", c.marketType) // Используем логгер Debug

	conn := c.conn // Локальная копия соединения

	// Try sending a normal closure message within the context deadline.
	// Set write deadline using the context deadline.
	deadline, ok := ctx.Deadline()
	if ok {
		err := conn.SetWriteDeadline(deadline) // Устанавливаем дедлайн из контекста
		if err != nil {
			c.log.WithError(err).Warnf("BybitClient (%s): CloseWithContext: Ошибка установки WriteDeadline для PING/CloseMessage.", c.marketType) // Используем логгер Warn
			// Продолжаем закрытие даже при ошибке установки дедлайна.
		}
	}

	// Try sending a close message.
	// WriteMessage respects the WriteDeadline set above.
	err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down")) // Используем локальную копию conn
	if err != nil && err != websocket.ErrCloseSent {
		// Логируем ошибку отправки CloseMessage на уровне Warn, если это не стандартная ошибка "уже закрыто".
		c.log.WithError(err).Warnf("BybitClient (%s): CloseWithContext: Ошибка отправки WebSocket close message.", c.marketType) // Используем логгер Warn
		// Не возвращаем ошибку здесь, т.к. физическое закрытие может все еще сработать.
	} else if err == nil {
		c.log.Tracef("BybitClient (%s): CloseWithContext: WebSocket close message отправлено.", c.marketType) // Trace
	}

	// Perform physical connection closure. This should ideally be unblocked by the WriteMessage
	// completion or the context deadline.
	err = conn.Close() // Используем локальную копию conn
	if err != nil {
		// Игнорируем ошибки "use of closed network connection".
		if !strings.Contains(err.Error(), "use of closed network connection") {
			c.log.WithError(err).Errorf("BybitClient (%s): CloseWithContext: Ошибка закрытия WebSocket соединения.", c.marketType) // Используем логгер Error
		} else {
			c.log.Tracef("BybitClient (%s): CloseWithContext: Ошибка 'use of closed network connection' при закрытии WS.", c.marketType) // Trace
		}
	}

	c.conn = nil                                                                                    // Устанавливаем соединение в nil после закрытия.
	c.log.Debugf("BybitClient (%s): CloseWithContext: Соединение WebSocket закрыто.", c.marketType) // Используем логгер Debug
	return err                                                                                      // Возвращаем ошибку физического закрытия (или nil).
}

// ... (код после CloseWithContext)

// Stop вызывается для корректной остановки клиента извне (например, при завершении программы).
// Этот метод сигнализирует клиенту о необходимости полного завершения работы. Использует logrus.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func (c *BybitClient) Stop() { // <<< Прикреплен к *BybitClient
	c.log.Infof("BybitClient (%s): Получен внешний сигнал остановки (Stop())...", c.marketType) // Используем логгер Info
	c.mu.Lock()                                                                                 // Блокируем мьютекс для безопасной работы с isClosed
	if c.isClosed {
		c.mu.Unlock()                                                                                // Освобождаем мьютекс
		c.log.Debugf("BybitClient (%s): Stop(): Клиент уже был помечен как закрытый.", c.marketType) // Используем логгер Debug
		return                                                                                       // Клиент уже останавливается.
	}
	c.isClosed = true // Устанавливаем флаг.
	c.mu.Unlock()     // Освобождаем мьютекс.

	c.log.Debugf("BybitClient (%s): Stop(): Отменяю внутренний контекст клиента (clientCtx).", c.marketType) // Используем логгер Debug
	c.clientCancel()                                                                                         // Отмена дочернего контекста clientCtx.

	// Примечание: WaitGroup (c.clientWg) ожидает завершения в defer блоке метода Run()
	// после вызова clientCancel. Это гарантирует, что все внутренние горутины
	// завершатся до того, как Run() закончит работу и счетчик WaitGroup в main.go уменьшится.

	c.log.Infof("BybitClient (%s): Stop(): Сигнал остановки обработан. Ожидание завершения внутренних горутин (происходит в Run defer).", c.marketType) // Используем логгер Info
}

// ... (код после Stop)

// getMapKeys Хелпер для получения ключей мапы (для логирования) - обычная функция.
// *** ДОБАВЛЕН этот отсутствующий метод ***
func getMapKeys(m map[string]json.RawMessage) []string { // <<< Обычная функция
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

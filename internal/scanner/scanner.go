// internal/scanner/scanner.go
package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"arbitrage-engine/internal/redis"
	"arbitrage-engine/internal/types"

	goredis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

// Scanner отвечает за получение данных от бирж через каналы,
// синхронизацию стаканов и BBO, и сохранение актуального состояния в Redis.
type Scanner struct {
	// Логгер с контекстом для данного компонента
	log *logrus.Entry

	// Контекст для управления жизненным циклом сканера
	// Этот контекст передается из main и отменяется при graceful shutdown.
	ctx context.Context

	// WaitGroup для отслеживания внутренних горутин Scanner'а (например, обработчиков буфера)
	scannerWg sync.WaitGroup // *** ДОБАВЛЕНО: WaitGroup для Scanner'а ***

	// Каналы для чтения данных от клиентов бирж (Общие каналы, куда пишут ВСЕ клиенты)
	orderBookMsgChan <-chan *types.OrderBookMessage // Канал для сообщений полного стакана (дельты/снимки)
	bboChan          <-chan *types.BBO              // Канал для сообщений BBO

	redisClient *redis.RedisClient // Клиент Redis из нашего локального пакета

	// Внутреннее состояние сканера (для отслеживания UpdateID и состояния синхронизации)
	// Ключ: Source:MarketType:Symbol (например, "binance:spot:BTCUSDT", "bybit:linear_futures:BTCUSDT")
	// Значение: структура с метаданными стакана
	orderBookMetadata map[string]*OrderBookSyncMetadata
	mu                sync.Mutex // Мьютекс для защиты доступа к map orderBookMetadata

	// Каналы для отправки команд ресинхронизации обратно клиентам.
	// Это map, где ключ - "source:market_type", значение - канал для записи в этот клиент.
	// Scanner будет писать в соответствующий канал при необходимости ресинка.
	// Тип значения в мапе - chan<- *types.ResyncCommand (Scanner в него только пишет)
	resyncCmdChannels map[string]chan<- *types.ResyncCommand // *** ИСПРАВЛЕНО: Тип канала теперь chan<- ***

	// Список символов, которые Scanner должен отслеживать.
	// Используется для фильтрации входящих сообщений.
	// Ключ: Symbol (uppercase)
	trackedSymbols map[string]bool

	// WaitGroup не хранится в структуре Scanner, а передается в Run и слушающие горутины.
	// WaitGroup *sync.WaitGroup // >>> Удаляем поле WaitGroup - DONE (заменено на scannerWg)
}

// OrderBookSyncMetadata хранит метаданные синхронизации для каждой пары Биржа:ТипРынка:Символ.
type OrderBookSyncMetadata struct {
	Source     string           // "binance", "bybit"
	MarketType types.MarketType // Тип рынка (spot, linear_futures, etc.)
	Symbol     string           // Символ в верхнем регистре
	// Поля для Binance (U/u ID)
	LastUpdateID int64 // Binance: The 'u' (FinalUpdateID) of the last processed delta or snapshot.
	// ProcessingSnapshot bool  // Удалено, статус CurrentStatus покрывает это
	ExpectedNextDeltaU int64                     // Binance: ожидаемый U следующей дельты в Synced статусе.
	BinanceDeltaBuffer []*types.OrderBookMessage // Буфер для Binance дельт, пришедших во время ожидания снимка.

	// Поля для Bybit (WS snapshot ID)
	BybitUpdateID int64 // Bybit V5: The 'u' (UpdateID) of the last processed message (snapshot or delta).
	BybitDepth    int   // Bybit V5: The depth of the subscribed order book (e.g., 1, 50, 500, 1000).

	// Общие поля
	LastUpdateTime time.Time // Время обработки последнего сообщения для этой пары
	CurrentStatus  string    // e.g., "Initializing", "Waiting for WS Delta", "Synced", "Gap Detected, Resyncing", "REST Snapshot Error", "Processing Buffer"
	// IsBuffering bool // Удалено, статус CurrentStatus достаточен (e.g., "Waiting for WS Delta")
	// TODO: Add more detailed status or timestamps for status transitions.
}

// >>> ДОБАВЛЕНА: Структура OrderBookInfo на уровне пакета
type OrderBookInfo struct {
	FinalUpdateID int64     `json:"final_update_id"`
	Timestamp     time.Time `json:"timestamp"`
	// TODO: Add BybitUpdateID or a generic ID field if needed for infoKey
	// Maybe a type-specific info struct keyed by MarketType?
	// For simplicity, let's keep a generic ID field for now, assuming Binance U/u is the primary sync ID.
	// If Bybit requires saving its specific ID format, we might need a different info structure or key.
}

// NewScanner создает новый экземпляр Scanner.
// Сигнатура обновлена: resyncChans теперь map[string]chan<- *types.ResyncCommand
// wg передается для запуска горутин слушателей общих каналов.
func NewScanner(
	ctx context.Context, // >>> Принимаем контекст - DONE
	log *logrus.Entry, // >>> Принимаем логгер *logrus.Entry - DONE
	obChan <-chan *types.OrderBookMessage,
	bboC <-chan *types.BBO,
	rClient *redis.RedisClient,
	// Принимаем мапу каналов ресинка, где ключ - "source:market_type".
	// Тип значения в мапе теперь chan<- *types.ResyncCommand (канал для записи)
	resyncChans map[string]chan<- *types.ResyncCommand, // *** ИСПРАВЛЕНО: Тип канала теперь chan<- ***
	symbols []string, // Список символов для отслеживания
	wg *sync.WaitGroup, // >>> Принимаем WaitGroup для запуска слушающих горутин - DONE
) *Scanner {

	// Логгируем начало инициализации сканера через переданный логгер
	log.Info("Начало инициализации Scanner...")

	// Создаем мапу отслеживаемых символов для быстрого поиска
	trackedSymbolsMap := make(map[string]bool)
	for _, s := range symbols {
		trackedSymbolsMap[strings.ToUpper(s)] = true // Save symbols in uppercase
	}
	log.WithField("symbols", symbols).WithField("count", len(trackedSymbolsMap)).Info("Scanner будет отслеживать символы:")

	// Initialize orderBookMetadata for every tracked symbol on every *enabled* exchange/market type
	obMetadata := make(map[string]*OrderBookSyncMetadata)
	initializedCount := 0
	// Iterate through the keys of the resync channels map ("source:market_type") provided by main.
	// This map contains entries ONLY for ENABLED exchange/market clients launched in main.
	for clientKey := range resyncChans {
		parts := strings.Split(clientKey, ":")
		if len(parts) != 2 {
			log.WithField("client_key", clientKey).Warn("Некорректный формат ключа клиента в мапе каналов ресинка. Пропускаю инициализацию метаданных для этого ключа.")
			continue
		}
		source := parts[0]
		// Safely convert string to MarketType
		var marketType types.MarketType
		switch strings.ToLower(parts[1]) {
		case string(types.MarketTypeSpot):
			marketType = types.MarketTypeSpot
		case string(types.MarketTypeLinearFutures):
			marketType = types.MarketTypeLinearFutures
		case string(types.MarketTypeCoinFutures):
			marketType = types.MarketTypeCoinFutures
		// TODO: Add other market types if needed
		default:
			log.WithField("market_type_str", parts[1]).WithField("client_key", clientKey).Warn("Неизвестный тип рынка в ключе клиента. Пропускаю инициализацию метаданных для этого ключа.")
			continue
		}

		// For each source+marketType, initialize metadata for all global tracked symbols.
		for symbol := range trackedSymbolsMap { // Iterate over the keys of the already created map (uppercase symbols)
			// Key format: source:market_type:symbol
			metadataKey := fmt.Sprintf("%s:%s:%s", source, marketType, symbol) // symbol is already uppercase
			obMetadata[metadataKey] = &OrderBookSyncMetadata{
				Source:             source,
				MarketType:         marketType,
				Symbol:             symbol, // Already uppercase
				LastUpdateID:       0,
				ExpectedNextDeltaU: 0,                                       // Binance specific
				BinanceDeltaBuffer: make([]*types.OrderBookMessage, 0, 100), // Initialize buffer slice (capacity)

				BybitUpdateID:  0, // Bybit specific
				BybitDepth:     0, // Bybit specific
				CurrentStatus:  "Initializing",
				LastUpdateTime: time.Time{},
			}
			initializedCount++
		}
	}
	log.WithField("metadata_count", initializedCount).Info("Инициализация метаданных стаканов завершена")

	scanner := &Scanner{
		log:               log, // >>> Сохраняем переданный логгер - DONE
		ctx:               ctx, // >>> Сохраняем переданный контекст - DONE
		orderBookMsgChan:  obChan,
		bboChan:           bboC,
		redisClient:       rClient,
		orderBookMetadata: obMetadata,
		resyncCmdChannels: resyncChans, // *** ИНИЦИАЛИЗАЦИЯ ПОЛЯ (chan<-) *** - DONE
		trackedSymbols:    trackedSymbolsMap,
		// scannerWg is zero-initialized.
	}

	// No need to launch listenForResyncCommands goroutines here.
	// The Scanner now writes to the channels, clients listen.
	log.Debug("Scanner: Слушатели каналов ресинка для клиентов НЕ запускаются в Scanner'е.")

	log.Info("Инициализация Scanner завершена.")

	return scanner
}

// Run запускает главный цикл обработки сообщений сканера.
// Принимает контекст и WaitGroup.
// *** ОБНОВЛЕННАЯ СИГНАТУРА ***
func (s *Scanner) Run(ctx context.Context, wg *sync.WaitGroup) { // >>> Принимает context, wg - DONE
	// wg.Add(1) is called in main.go before launching this goroutine.
	// Уменьшаем счетчик wg при выходе из горутины.
	defer wg.Done() // >>> Используем ПЕРЕДАННЫЙ wg - FIXED

	s.log.Info("Горутина Scanner Run запущена.")
	defer func() {
		s.log.Info("Scanner Run: Ожидание завершения внутренних горутин Scanner'а...")
		s.scannerWg.Wait() // Ожидаем завершения всех внутренних горутин Scanner'а
		s.log.Info("Горутина Scanner Run остановлена.")
	}()

	// Локальные переменные для каналов, чтобы можно было установить их в nil после закрытия
	obMsgChanLocal := s.orderBookMsgChan
	bboChanLocal := s.bboChan

	s.log.Info("Scanner: Ожидание данных из общих каналов или сигнала отмены контекста...")

scannerLoop:
	for {
		select {
		case msg, ok := <-obMsgChanLocal:
			if !ok {
				s.log.Info("Scanner: orderBookMsgChan закрыт. Установка в nil.")
				obMsgChanLocal = nil
				// Если оба канала закрыты, выходим из основного цикла
				if obMsgChanLocal == nil && bboChanLocal == nil {
					s.log.Info("Scanner: Оба канала данных закрыты. Инициирование выхода из основного select loop.")
					break scannerLoop
				}
				continue // Переход к следующей итерации select
			}
			// Обработка сообщения стакана
			// processOrderBookMessage теперь управляет блокировкой s.mu и вызывает отдельные логические функции.
			s.processOrderBookMessage(msg)

		case bbo, ok := <-bboChanLocal:
			if !ok {
				s.log.Info("Scanner: bboChan закрыт. Установка в nil.")
				bboChanLocal = nil
				// Если оба канала закрыты, выходим из основного цикла
				if obMsgChanLocal == nil && bboChanLocal == nil {
					s.log.Info("Scanner: Оба канала данных закрыты. Инициирование выхода из основного select loop.")
					break scannerLoop
				}
				continue // Переход к следующей итерации select
			}
			// Обработка сообщения BBO
			// processBBO не запускает горутин и не требует особого управления мьютексом Scanner'а,
			// только для доступа к RedisClient (который потокобезопасен) и s.trackedSymbols (который неизменен).
			s.processBBO(bbo) // >>> Метод processBBO не принимает wg. В нем нет запуска горутин. Все Ок.

		case <-ctx.Done(): // Слушаем контекст, переданный в эту горутину (контекст Scanner'а)
			s.log.Info("Scanner: Получен сигнал отмены контекста. Инициирование выхода из основного select loop.")
			break scannerLoop // Контекст отменен, инициируем выход

		case <-time.After(10 * time.Second): // Периодический статус лог (увеличен интервал)
			// Используем логгер с контекстом компонента, добавляя поле "report_type"
			s.log.WithField("report_type", "status_report").Info("--- Отчет о состоянии Scanner ---")
			s.mu.Lock() // Блокируем для безопасного доступа к метаданным
			if len(s.orderBookMetadata) == 0 {
				s.log.WithField("report_type", "status_report").Info("Метаданные стаканов еще не инициализированы.")
			} else {
				// Итерируем по метаданным и логируем статус для каждой пары source:market_type:symbol
				// Используем поля логгера для структурированного вывода
				for key, md := range s.orderBookMetadata {
					s.log.WithFields(logrus.Fields{
						"report_type":      "status_report",
						"market_key":       key, // source:market_type:symbol
						"status":           md.CurrentStatus,
						"last_binance_id":  md.LastUpdateID,       // Can be 0 if not synced or during resync
						"expected_next_U":  md.ExpectedNextDeltaU, // Binance specific
						"bybit_id":         md.BybitUpdateID,      // Bybit specific
						"bybit_depth":      md.BybitDepth,         // Bybit specific
						"last_update_time": md.LastUpdateTime.Format(time.RFC3339Nano),
						"buffer_size":      len(md.BinanceDeltaBuffer), // Binance specific
					}).Info("Состояние стакана")
				}
			}
			s.log.WithField("report_type", "status_report").Info("----------------------------------")
			s.mu.Unlock() // Разблокируем
		}
	}

	// Логирование после выхода из основного цикла select
	s.log.Info("Scanner: Основной цикл select завершен.")
	s.log.Info("Scanner: Слив оставшихся сообщений из закрытых каналов (если есть)...")

	// Сливаем оставшиеся сообщения в буферах каналов после того, как они будут закрыты в main.go.
	// Эти range-циклы завершатся, как только каналы станут пустыми и закрытыми.
	s.log.Debug("Scanner: Слив orderBookMsgChan...") // Уровень Debug для деталей завершения
	for msg := range s.orderBookMsgChan {
		s.processOrderBookMessage(msg) // Обрабатываем слитое сообщение
	}
	s.log.Debug("Scanner: Слив orderBookMsgChan завершен.")

	s.log.Debug("Scanner: Слив bboChan...") // Уровень Debug для деталей завершения
	for bbo := range s.bboChan {
		s.processBBO(bbo) // Обрабатываем слитое сообщение
	}
	s.log.Debug("Scanner: Слив bboChan завершен.")

	// Run defer now waits for scannerWg.
}

// listenForResyncCommands удалена, Scanner теперь только пишет в каналы ресинка.
// handleReceivedResyncCommand удалена, Scanner не обрабатывает команды ресинка, которые сам отправляет.

// processOrderBookMessage обрабатывает входящее сообщение стакана.
// Управляет мьютексом s.mu и вызывает специфичную логику для биржи.
// Выполнение Redis операций происходит после разблокировки мьютекса (если данные применены).
func (s *Scanner) processOrderBookMessage(msg *types.OrderBookMessage) {
	// Используем логгер с контекстом сообщения
	logger := s.log.WithFields(logrus.Fields{
		"event":       "process_order_book",
		"source":      msg.Source,
		"market_type": msg.MarketType,
		"symbol":      msg.Symbol,
		"msg_type":    msg.Type,
	})

	// --- ПРОВЕРКИ ВАЛИДНОСТИ СООБЩЕНИЯ ---
	if msg == nil || msg.Delta == nil {
		logger.Warn("Получено nil OrderBookMessage или Delta == nil. Игнорирование.")
		return
	}
	// Проверка на пустые обязательные поля
	if msg.Symbol == "" || msg.Source == "" || msg.MarketType == "" {
		logger.WithFields(logrus.Fields{
			"source": msg.Source, "market_type": msg.MarketType, "symbol": msg.Symbol,
		}).Warn("Получено OrderBookMessage с недостаточной информацией (Source, MarketType, Symbol). Игнорирование.")
		return
	}

	// Key for metadata and Redis state: source:market_type:symbol
	// Используем uppercase символ для ключа метаданных
	upperSymbol := strings.ToUpper(msg.Symbol)
	metadataKey := fmt.Sprintf("%s:%s:%s", msg.Source, msg.MarketType, upperSymbol)

	// --- БЛОКИРОВКА МЬЮТЕКСА И ПОЛУЧЕНИЕ МЕТАДАННЫХ ---
	s.mu.Lock()
	metadata, ok := s.orderBookMetadata[metadataKey]
	if !ok {
		// Метаданные для этой пары не найдены. Игнорируем сообщение.
		s.mu.Unlock() // Разблокируем перед выходом
		logger.WithField("metadata_key", metadataKey).Error("Получено OrderBookMessage, но метаданные для пары не найдены. Пропущено сообщение. Проверьте инициализацию Scanner.")
		return
	}
	// Мьютекс s.mu заблокирован. Метаданные получены.

	// Логируем текущее состояние метаданных перед обработкой сообщения
	logger.WithFields(logrus.Fields{
		"current_status":  metadata.CurrentStatus,
		"current_last_id": metadata.LastUpdateID,
		"expected_next_U": metadata.ExpectedNextDeltaU,
		"bybit_id":        metadata.BybitUpdateID,           // Bybit specific
		"buffer_size":     len(metadata.BinanceDeltaBuffer), // Binance specific
		"msg_update_id":   msg.Delta.FinalUpdateID,
	}).Trace("Обработка сообщения для пары с метаданными.")

	var redisUpdateNeeded bool // Флаг, указывающий, нужно ли обновить Redis после обработки логики.
	var resyncNeeded bool      // Флаг, указывающий, требуется ли повторная синхронизация

	// --- ВЫЗОВ СПЕЦИФИЧНОЙ ЛОГИКИ БИРЖИ (под мьютексом) ---
	// Эти функции ТОЛЬКО обновляют metadata и буфер.
	switch msg.Source {
	case "binance":
		// processBinanceOrderBookLogic обновляет метаданные и буфер под s.mu.
		// Она также решает, нужно ли запустить обработчик буфера или инициировать ресинк.
		// Возвращает флаг redisUpdateNeeded.
		redisUpdateNeeded = s.processBinanceOrderBookLogic(metadata, msg)
		// После вызова логики, проверяем статус метаданных (который мог быть изменен логикой)
		// для определения, нужен ли ресинк.
		if metadata.CurrentStatus == "Gap Detected, Resyncing" {
			resyncNeeded = true // Ресинк нужен, если логика установила такой статус
		}

	case "bybit":
		// processBybitOrderBookLogic обновляет метаданные под s.mu.
		// Возвращает флаг redisUpdateNeeded.
		redisUpdateNeeded = s.processBybitOrderBookLogic(metadata, msg) // Адаптируем сигнатуру
		// После вызова логики, проверяем статус метаданных для определения, нужен ли ресинк.
		if metadata.CurrentStatus == "Gap Detected, Resyncing" {
			resyncNeeded = true // Ресинк нужен, если логика установила такой статус
		}

	default:
		logger.WithField("source", msg.Source).Error("Неизвестный источник стакана для сообщения. Игнорирование.")
		metadata.CurrentStatus = "Error: Unknown Source" // Обновляем статус
		redisUpdateNeeded = false                        // Не обновляем Redis для неизвестного источника
		resyncNeeded = false                             // Не инициируем ресинк
	}

	// --- РАЗБЛОКИРОВКА МЬЮТЕКСА ---
	s.mu.Unlock() // Разблокируем мьютекс после всех манипуляций с метаданными и буфером.

	// --- ВЫПОЛНЕНИЕ REDIS ОПЕРАЦИЙ (вне мьютекса) ---
	if redisUpdateNeeded {
		// Если специфичная логика решила, что данные сообщения нужно применить к Redis...
		// (например, это последовательная дельта или snapshot)
		logger.Trace("Требуется обновление Redis. Вызов updateOrderBookInRedisZSets.")
		// updateOrderBookInRedisZSets не требует мьютекса s.mu.
		// Передаем данные из сообщения.
		// Важно: для snapshot_rest, Delta уже содержит все уровни снимка.
		// Для delta, Delta содержит только изменения.
		// LastUpdateID/FinalUpdateID и Timestamp из сообщения (или из metadata после обновления)
		// нужно передать для сохранения в infoKey.
		// Используем FinalUpdateID и Timestamp из message.Delta.
		s.updateOrderBookInRedisZSets(msg.Source, msg.MarketType, upperSymbol, msg.Delta.Bids, msg.Delta.Asks, msg.Delta.FinalUpdateID, msg.Delta.Timestamp)
	}

	// --- ИНИЦИАЦИЯ РЕСИНКА (вне мьютекса) ---
	// Если resyncNeeded установлен специфичным обработчиком (логикой),
	// отправляем команду ресинка.
	// ЭТОТ БЛОК КОДА ДОЛЖЕН БЫТЬ ВЫЗВАН ПОСЛЕ s.mu.Unlock().
	if resyncNeeded {
		logger.Info("Обнаружен разрыв или ошибка. Требуется ресинхронизация. Подготовка к отправке команды.")
		// Определяем тип команды ресинка в зависимости от источника
		var commandType types.ResyncCommandType
		switch msg.Source {
		case "binance":
			commandType = types.ResyncTypeBinanceSnapshot
		case "bybit":
			commandType = types.ResyncTypeBybitResubscribe // Bybit использует реподписку WS
		default:
			logger.WithField("source", msg.Source).Error("Неизвестный источник для определения типа команды ресинка.")
			return // Не можем отправить команду
		}

		s.sendResyncCommand(msg.Source, msg.MarketType, upperSymbol, commandType, logger)
	}
	// Конец метода processOrderBookMessage. Мьютекс s.mu уже разблокирован.
}

// processBinanceOrderBookLogic содержит специфичную логику синхронизации для Binance.
// Она вызывается из processOrderBookMessage ПОД БЛОКИРОВКОЙ s.mu.
// Обновляет метаданные (metadata) и буфер (metadata.BinanceDeltaBuffer) напрямую.
// НЕ вызывает Redis операции и НЕ отправляет команды ресинка (это делает вызывающий или worker).
// Возвращает bool: true, если данные из сообщения нужно применить к Redis; false иначе.
// *** НОВАЯ ФУНКНЦИЯ / ИСПРАВЛЕННАЯ ЛОГИКА БУФЕРА И ПРИМЕНЕНИЯ СНИМКА ***
func (s *Scanner) processBinanceOrderBookLogic(metadata *OrderBookSyncMetadata, msg *types.OrderBookMessage) (redisUpdateNeeded bool) {
	// Используем логгер с контекстом, добавляя update_id для отладки
	logger := s.log.WithFields(logrus.Fields{
		"event":           "process_binance_ob_logic",
		"symbol":          metadata.Symbol, // Символ уже в верхнем регистре
		"market_type":     metadata.MarketType,
		"msg_type":        msg.Type,
		"msg_update_id":   msg.Delta.FinalUpdateID, // 'u' для delta, LastUpdateId для snapshot
		"current_last_id": metadata.LastUpdateID,
		"current_status":  metadata.CurrentStatus,
		"expected_next_U": metadata.ExpectedNextDeltaU,
		"buffer_size":     len(metadata.BinanceDeltaBuffer),
	})
	logger.Trace("Начало логики обработки Binance OrderBook сообщения.")

	// Эта функция выполняется под s.mu.
	// НЕ вызываем здесь Redis операции и НЕ отправляем команды ресинка напрямую.
	// Обновляем metadata и буфер.

	// redisUpdateNeeded по умолчанию false
	redisUpdateNeeded = false

	switch msg.Type {
	case "snapshot_rest":
		// Получен REST snapshot. Это всегда инициирует процесс синхронизации.
		snapshotID := msg.Delta.FinalUpdateID

		logger.WithFields(logrus.Fields{
			"snapshot_id":    snapshotID,
			"bids_count":     len(msg.Delta.Bids),
			"asks_count":     len(msg.Delta.Asks),
			"current_status": metadata.CurrentStatus,
			"buffer_size":    len(metadata.BinanceDeltaBuffer),
		}).Info("Получен REST snapshot. Обновление метаданных и запуск обработчика буфера.")

		// 1. Обновить in-memory metadata для отражения нового снимка.
		metadata.LastUpdateID = snapshotID // Устанавливаем ID снимка
		// Последнее время обновления будет установлено после успешной обработки буфера или если буфер пуст.
		// metadata.LastUpdateTime = time.Now() // Не обновляем время сразу, ждем буфера
		metadata.ExpectedNextDeltaU = snapshotID + 1 // Ожидаем следующую дельту сразу после снимка.
		metadata.CurrentStatus = "Processing Buffer" // Устанавливаем статус обработки буфера

		// 2. Запустить горутину для обработки буфера дельт, пришедших во время запроса снимка.
		//    Эта горутина будет сама управлять блокировкой s.mu для доступа к метаданным
		//    и отправке команды ресинка, если обнаружит разрыв в буфере.
		//    Она также очистит буфер metadata.BinanceDeltaBuffer в основной структуре.
		//    Передаем logger для контекста.

		s.scannerWg.Add(1)                                                                                                                                      // Увеличиваем счетчик WaitGroup Scanner'а
		go s.processBufferedBinanceDeltasWorker(metadata.Source, metadata.MarketType, metadata.Symbol, snapshotID, logger.WithField("symbol", metadata.Symbol)) // Pass Source, MarketType, Symbol, snapshotID and logger. Worker gets access to s.mu etc.

		// 3. Применить snapshot уровни к Redis ZSETs.
		//    Это нужно сделать после разблокировки мьютекса.
		//    processOrderBookMessage вызовет updateOrderBookInRedisZSets с данными этого сообщения.
		redisUpdateNeeded = true // Сигнализируем, что данные снимка нужно применить к Redis.

		// Ресинк не требуется СРАЗУ после получения снимка.
		// Ресинк будет инициирован ИЗ ГОРУТИНЫ processBufferedBinanceDeltasWorker, если она обнаружит разрыв.
		return redisUpdateNeeded // Возвращаем true, чтобы processOrderBookMessage обновил Redis

	case "delta":
		// Получена дельта. Обрабатываем ее в зависимости от текущего статуса.
		deltaU := msg.Delta.FirstUpdateID // 'U' из delta.
		deltau := msg.Delta.FinalUpdateID // 'u' из delta.

		logger = logger.WithFields(logrus.Fields{
			"delta_U":         deltaU,
			"delta_u":         deltau,
			"current_last_id": metadata.LastUpdateID, // LastUpdateID может быть 0, snapshotID или u предыдущей дельты
			"current_status":  metadata.CurrentStatus,
			"expected_next_U": metadata.ExpectedNextDeltaU, // Expected next U
			"buffer_size":     len(metadata.BinanceDeltaBuffer),
		}) // Добавляем поля для этой ветки

		// Игнорируем старые дельты.
		// Если мы в "Initializing", LastUpdateID=0, не игнорируем (буферизация).
		// Если LastUpdateID > 0, игнорируем дельты с u <= LastUpdateID.
		if metadata.LastUpdateID > 0 && deltau <= metadata.LastUpdateID {
			// logger.Trace("Игнорирование старой дельты.") // Очень шумно
			return false // Игнорируем старое сообщение, Redis update not needed.
		}

		// --- ЛОГИКА БУФЕРИЗАЦИИ ---
		// Если мы находимся в статусе, требующем буферизации (Initializing, Waiting for WS Delta, Gap Detected, Resyncing, Processing Buffer)...
		// Буферизируем дельты.
		if metadata.CurrentStatus == "Initializing" ||
			metadata.CurrentStatus == "Waiting for WS Delta" ||
			metadata.CurrentStatus == "Gap Detected, Resyncing" ||
			metadata.CurrentStatus == "Processing Buffer" {

			logger.WithFields(logrus.Fields{
				"delta_U": deltaU, "delta_u": deltau, "buffer_size_before": len(metadata.BinanceDeltaBuffer), "current_status": metadata.CurrentStatus,
			}).Trace("Буферизация входящей дельты.")

			// Добавляем сообщение в буфер.
			// TODO: Ограничить размер буфера!
			metadata.BinanceDeltaBuffer = append(metadata.BinanceDeltaBuffer, msg)

			// Статус обновляется при получении снимка или обнаружении разрыва.
			// Флаг IsBuffering не нужен, т.к. CurrentStatus="Waiting for WS Delta" или "Processing Buffer"
			// или "Gap Detected, Resyncing" указывает на буферизацию.

			return false // Дельта добавлена в буфер, не обрабатываем ее сейчас, Redis update not needed.
		}
		// --- КОНЕЦ ЛОГИКИ БУФЕРИЗАЦИИ ---

		// --- ЛОГИКА ОБЫЧНОЙ ПОСЛЕДОВАТЕЛЬНОЙ ОБРАБОТКИ ДЕЛЬТ (Статус Synced) ---
		// Если мы достигли этой точки, значит, CurrentStatus == "Synced".
		// ExpectedNextDeltaU должен быть установлен (metadata.LastUpdateID + 1).

		expectedU := metadata.LastUpdateID + 1 // Ожидаемое U следующей дельты.

		logger = logger.WithField("expected_U", expectedU) // Добавляем поле

		if deltaU == expectedU {
			// Последовательность сохранена. Обновляем метаданные.
			metadata.LastUpdateID = deltau           // Обновляем LastUpdateID на 'u' текущей дельты.
			metadata.LastUpdateTime = time.Now()     // Обновляем время.
			metadata.ExpectedNextDeltaU = deltau + 1 // Обновляем ожидаемый U.
			// Статус остается Synced.

			logger.WithFields(logrus.Fields{"delta_U": deltaU, "delta_u": deltau, "new_last_id": metadata.LastUpdateID}).Trace("Последовательная дельта. Метаданные обновлены.")
			redisUpdateNeeded = true // Сигнализируем, что дельту нужно применить к Redis.
			return redisUpdateNeeded

		} else if deltau >= expectedU { // *** ИЗМЕНЕНО: Если u >= expectedU НО U != expectedU, это разрыв ***
			// Обнаружен разрыв в последовательности дельт. Это происходит, если deltaU != expectedU
			// (т.е. deltaU > expectedU или deltaU < expectedU, при условии deltau > current_last_id,
			// что уже проверено в самом начале функции).
			// Если deltau < expectedU, это старая дельта, уже отфильтрованная выше.
			logger.WithFields(logrus.Fields{
				"delta_U": deltaU, "delta_u": deltau, "expected_U": expectedU, "current_last_id": metadata.LastUpdateID,
			}).Error("Обнаружен разрыв в последовательности дельт в Synced статусе (U != expected_U)! Инициирование ресинка.")
			// Сбрасываем метаданные для инициирования нового цикла синхронизации.
			metadata.LastUpdateID = 0                                             // Сброс ID на 0 при ожидании нового снимка.
			metadata.ExpectedNextDeltaU = 0                                       // Сбрасываем ожидаемый U.
			metadata.CurrentStatus = "Gap Detected, Resyncing"                    // Обновляем статус.
			metadata.BinanceDeltaBuffer = make([]*types.OrderBookMessage, 0, 100) // Очищаем буфер для начала новой буферизации.
			// Текущая дельта, вызвавшая разрыв, НЕ добавляется в буфер ЗДЕСЬ.
			// Она придет снова (или последующие) и будет добавлена в буфер, когда статус станет "Resyncing".

			redisUpdateNeeded = false // Не применяем текущую дельту к Redis.
			// Ресинк будет инициирован processOrderBookMessage после разблокировки s.mu.
			return redisUpdateNeeded // Возвращает false, processOrderBookMessage увидит статус и отправит ресинк.

		} else { // deltau < expectedU - должно быть отфильтровано проверкой deltau <= metadata.LastUpdateID
			// Это не должно происходить при правильной фильтрации старых дельт.
			logger.WithFields(logrus.Fields{
				"delta_U": deltaU, "delta_u": deltau, "expected_U": expectedU, "current_last_id": metadata.LastUpdateID,
				"check_failed": "deltau < expectedU after deltau > current_last_id check",
			}).Warn("Неожиданная дельта Binance. Возможно, ошибка фильтрации старых дельт или биржи. Игнорирование.")
			// TODO: Возможно, это тоже должно приводить к ресинку? Зависит от поведения биржи.
			// Binance API docs suggest U/u sequence is strict. A delta with u > last_u but U < last_u+1 is a gap.
			// So this case *is* a gap. Let's treat it as such.
			logger.WithFields(logrus.Fields{
				"delta_U": deltaU, "delta_u": deltau, "expected_U": expectedU, "current_last_id": metadata.LastUpdateID,
			}).Error("Обнаружен разрыв в последовательности дельт (u > current_last_id, но U < expected_U)! Инициирование ресинка.")
			metadata.LastUpdateID = 0
			metadata.ExpectedNextDeltaU = 0
			metadata.CurrentStatus = "Gap Detected, Resyncing"
			metadata.BinanceDeltaBuffer = make([]*types.OrderBookMessage, 0, 100)

			redisUpdateNeeded = false // Не применяем к Redis.
			return redisUpdateNeeded  // Возвращает false, processOrderBookMessage увидит статус и отправит ресинк.
		}

	case "bookTicker":
		// BookTicker сообщения обрабатываются processBBO.
		logger.Warn("Получено сообщение типа 'bookTicker' через OrderBookChan. Этого не должно происходить. Игнорирование.")
		metadata.CurrentStatus = "Warning: BookTicker in OB Chan"
		return false // Не применяем к Redis

	default:
		logger.WithField("msg_type", msg.Type).Error("Получено сообщение с неизвестным типом. Игнорирование.")
		metadata.CurrentStatus = fmt.Sprintf("Unknown Message Type: %s", msg.Type)
		return false // Не применяем к Redis
	}
}

// processBufferedBinanceDeltasWorker обрабатывает буфер дельт после получения снимка в отдельной горутине.
// Она управляет блокировкой s.mu при доступе к метаданным (чтение буфера, очистка, обновление статуса).
// Обновляет Redis напрямую (вызывая метод Scanner'а updateOrderBookInRedisZSets, который не требует s.mu).
// Отправляет команду ресинка, если обнаруживает разрыв в буфере (вызывая метод Scanner'а sendResyncCommand).
// Принимает символьные данные, snapshot_id, и логгер. Получает доступ к остальным полям Scanner'а через указатель s.
// *** НОВАЯ ГОУРУТИНА/ФУНКЦИЯ ***
func (s *Scanner) processBufferedBinanceDeltasWorker(source string, marketType types.MarketType, symbol string, snapshotID int64, logger *logrus.Entry) {
	// Исправлено: Использование source, marketType, symbol в логгере.
	logger = logger.WithFields(logrus.Fields{
		"func":        "processBufferedBinanceDeltasWorker",
		"source":      source,     // Использовано в логгере
		"market_type": marketType, // Использовано в логгере
		"symbol":      symbol,     // Symbol is uppercase here, used in logger
		"snapshot_id": snapshotID, // ID снимка, относительно которого начинаем обработку
	})
	logger.Info("Запущена горутина обработчика буфера дельт.")
	defer logger.Info("Горутина обработчика буфера дельт завершена.")

	// === ЭТА ГОУРУТИНА НЕ ВЫХОДИТ ДО ПОЛНОЙ ОБРАБОТКИ БУФЕРА ИЛИ ОБНАРУЖЕНИЯ РАЗРЫВА ===

	var bufferToProcess []*types.OrderBookMessage
	metadataKey := fmt.Sprintf("%s:%s:%s", source, marketType, symbol) // Use passed symbol, which is uppercase

	// === 1. Копируем буфер и очищаем основной буфер в метаданных (под блокировкой) ===
	s.mu.Lock() // Блокируем для доступа к метаданным
	metadata, ok := s.orderBookMetadata[metadataKey]
	if !ok {
		s.mu.Unlock() // Разблокируем перед выходом
		logger.WithField("metadata_key", metadataKey).Error("Метаданные для пары не найдены в обработчике буфера. Невозможно обработать буфер.")
		return // Невозможно обработать буфер без метаданных
	}

	// Проверка sanity: ID снимка в метаданных должно совпадать с переданным snapshotID
	// ( metadata.LastUpdateID был установлен на snapshotID в processBinanceOrderBookLogic )
	if metadata.LastUpdateID != snapshotID {
		s.mu.Unlock() // Разблокируем перед выходом
		logger.WithFields(logrus.Fields{
			"metadata_snapshot_id": metadata.LastUpdateID, "passed_snapshot_id": snapshotID,
		}).Error("Несоответствие snapshotID в метаданных и переданного в обработчик буфера. Ошибка логики. Пропуск обработки буфера.")
		// Статус метаданных, вероятно, уже некорректен, не пытаемся его обновлять здесь.
		return
	}

	// Копируем буфер.
	bufferToProcess = make([]*types.OrderBookMessage, len(metadata.BinanceDeltaBuffer))
	copy(bufferToProcess, metadata.BinanceDeltaBuffer)

	// Очищаем буфер в основной структуре метаданных.
	metadata.BinanceDeltaBuffer = make([]*types.OrderBookMessage, 0, 100) // Очищаем и сбрасываем емкость

	// Статус уже установлен в "Processing Buffer" в processBinanceOrderBookLogic
	// metadata.CurrentStatus = "Processing Buffer" // Статус установлен ранее

	s.mu.Unlock() // Разблокируем после копирования и очистки буфера

	logger.WithField("buffer_size_copied", len(bufferToProcess)).Debug("Буфер скопирован и очищен в метаданных. Начало обработки логики буфера.")

	// === 2. Выполняем логику обработки скопированного буфера ===
	// Эта функция ТОЛЬКО работает с локальной копией буфера и ID снимка,
	// она НЕ требует s.mu и НЕ обращается к s.orderBookMetadata напрямую.
	// Она вызывает s.updateOrderBookInRedisZSets, который не требует s.mu.
	bufferProcessedSuccessfully, lastProcessedUInBuffer := s.processBufferedBinanceDeltasLogic(bufferToProcess, snapshotID, logger) // <<< ВЫЗОВ ЛОГИКИ

	// === 3. Обновляем метаданные Scanner'а и, если нужно, отправляем команду ресинка ===
	s.mu.Lock()         // Блокируем снова для обновления метаданных
	defer s.mu.Unlock() // Гарантируем разблокировку при выходе из горутины worker'а

	// Переполучаем метаданные, т.к. указатель мог стать невалидным или структура могла быть заменена
	// (хотя при работе под мьютексом это маловероятно, но безопаснее)
	metadata, ok = s.orderBookMetadata[metadataKey]
	if !ok {
		logger.WithField("metadata_key", metadataKey).Error("Метаданные для пары не найдены при завершении обработки буфера. Ошибка логики.")
		return // Невозможно обновить статус
	}

	if bufferProcessedSuccessfully {
		// Буфер успешно применен поверх снимка.
		// LastUpdateID теперь равно ID последнего успешно примененного сообщения (из буфера или снимка, если буфер пуст).
		metadata.LastUpdateID = lastProcessedUInBuffer          // Устанавливаем последний обработанный ID
		metadata.LastUpdateTime = time.Now()                    // Обновляем время после обработки буфера
		metadata.ExpectedNextDeltaU = metadata.LastUpdateID + 1 // Устанавливаем ожидаемый U для следующих дельт.
		metadata.CurrentStatus = "Synced"                       // Переход в статус Synced.

		logger.WithFields(logrus.Fields{
			"new_last_id": metadata.LastUpdateID,
			"new_status":  metadata.CurrentStatus,
		}).Info("Обработка буфера дельт завершена. Синхронизация успешна.")

	} else {
		// Обнаружен разрыв ВНУТРИ БУФЕРА или буфер не смог "догнать" снимок.
		logger.Warn("Обработка буфера дельт после снимка обнаружила разрыв. Требуется повторный ресинк.")
		// Статус и ExpectedNextDeltaU уже установлены в processBufferedBinanceDeltasLogic.
		metadata.CurrentStatus = "Gap Detected, Resyncing" // Устанавливаем статус.
		metadata.LastUpdateID = 0                          // Сбрасываем ID, чтобы ждать новый снимок.
		metadata.ExpectedNextDeltaU = 0                    // Сбрасываем ожидаемый U.
		// Буфер уже очищен ранее.

		// Сигнализируем, что требуется повторный ресинк, отправляя команду клиенту.
		logger.Warn("Обнаружен разрыв во время обработки буфера. Запуск команды ресинка.")
		// Отправляем команду ресинка через Scanner'а. Это делается ВНУТРИ ЭТОЙ горутины worker'а.
		// Метод sendResyncCommand сам запускает новую горутину для отправки команды и добавляет ее в scannerWg.
		s.sendResyncCommand(metadata.Source, metadata.MarketType, metadata.Symbol, types.ResyncTypeBinanceSnapshot, logger) // <<< ВЫЗОВ МЕТОДА ОТПРАВКИ КОМАНДЫ
	}
	// Мьютекс s.mu разблокирован defer'ом.
}

func (s *Scanner) GetOrderBookLevels(ctx context.Context, source string, marketType types.MarketType, symbol string, limit int) ([]types.PriceLevel, []types.PriceLevel, int64, time.Time, error) { // <<< Прикреплен к *Scanner, принимает context
	upperSymbol := strings.ToUpper(symbol)
	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":        "GetOrderBookLevels",
		"source":      source,
		"market_type": marketType,
		"symbol":      upperSymbol,
		"limit":       limit,
	})
	logger.Debug("Извлечение уровней стакана из Redis.") // Используем логгер Debug

	// Check context cancellation before starting Redis operations
	select {
	case <-ctx.Done():
		logger.Warn("Context cancelled during GetOrderBookLevels. Aborting Redis operations.")
		return nil, nil, 0, time.Time{}, ctx.Err() // Return context error
	default:
		// Continue
	}

	// Get keys using the helper function that includes market type.
	// These variables ARE used below in the Redis pipeline commands.
	bidsKey, asksKey, volumeKey, infoKey := getOrderBookZSetKeys(source, marketType, upperSymbol) // getOrderBookZSetKeys is defined below

	pipe := s.redisClient.Client.Pipeline()

	// Get top N Bids (descending price)
	bidMembersCmd := pipe.ZRevRangeWithScores(ctx, bidsKey, 0, int64(limit)-1) // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx

	// Get top N Asks (ascending price)
	askMembersCmd := pipe.ZRangeWithScores(ctx, asksKey, 0, int64(limit)-1) // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx

	// Get last update info (JSON string)
	infoCmd := pipe.Get(ctx, infoKey) // Info is stored as a JSON string // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx

	// Execute the pipeline
	_, err := pipe.Exec(ctx) // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx
	// Handle general pipeline errors (excluding goredis.Nil for missing keys)
	if err != nil && err != goredis.Nil {
		logger.WithError(err).Error("Ошибка выполнения Redis pipeline для получения уровней стакана.") // Используем логгер Error
		return nil, nil, 0, time.Time{}, fmt.Errorf("Scanner: Error getting order book levels from Redis for %s:%s:%s: %w", source, marketType, upperSymbol, err)
	}

	// --- Processing results ---

	var lastUpdateID int64
	var lastUpdateTime time.Time

	// Process infoCmd result (handles goredis.Nil internally)
	infoJson, infoErr := infoCmd.Result()
	if infoErr != nil {
		// infoErr == goredis.Nil - это нормальная ситуация, если ключ infoKey не существует.
		// Другие ошибки логируем.
		if infoErr != goredis.Nil {
			logger.WithError(infoErr).Warn("Ошибка получения info из Redis.") // Используем логгер Warn
		} else {
			logger.Trace("Ключ infoKey не найден в Redis.") // Trace, т.к. это может быть при первом запуске
		}
		// If info is missing (goredis.Nil) or error, use default values (0, time.Time{}).
	} else { // infoJson успешно получен
		var infoData OrderBookInfo // Используем ту же структуру, что и при сохранении (OrderBookInfo defined above)
		// Ожидаем JSON строку типа {"final_update_id": 123, "timestamp": "..."}
		if err := json.Unmarshal([]byte(infoJson), &infoData); err != nil {
			logger.WithError(err).WithField("info_json", infoJson).Error("Ошибка парсинга JSON метаданных из Redis.") // Используем логгер Error
			// Ошибка парсинга, используем значения по умолчанию.
		} else {
			lastUpdateID = infoData.FinalUpdateID
			lastUpdateTime = infoData.Timestamp
			logger.WithFields(logrus.Fields{
				"retrieved_id":   lastUpdateID,
				"retrieved_time": lastUpdateTime,
			}).Trace("Метаданные стакана успешно извлечены и распарсены.") // Trace
		}
	}

	// Process bidMembersCmd and askMembersCmd results (handles goredis.Nil internally)
	bidMembers, bidErr := bidMembersCmd.Result()
	if bidErr != nil && bidErr != goredis.Nil {
		logger.WithError(bidErr).Error("Ошибка получения топ Bids из Redis.") // Используем логгер Error
		// If error getting Bids (not Nil), consider it a problem and return error
		return nil, nil, 0, time.Time{}, fmt.Errorf("Scanner: Error getting top Bids from Redis for %s:%s:%s: %w", source, marketType, upperSymbol, bidErr)
	}
	// Если bidErr == goredis.Nil, bidMembers будет пустым срезом []goredis.Z{}

	askMembers, askErr := askMembersCmd.Result()
	if askErr != nil && askErr != goredis.Nil {
		logger.WithError(askErr).Error("Ошибка получения топ Asks из Redis.") // Используем логгер Error
		// If error getting Asks (not Nil), consider it a problem and return error
		return nil, nil, 0, time.Time{}, fmt.Errorf("Scanner: Error getting top Asks from Redis for %s:%s:%s: %w", source, marketType, upperSymbol, askErr)
	}
	// Если askErr == goredis.Nil, askMembers будет пустым срезом []goredis.Z{}

	// Collect all prices (ZSET members) to fetch volumes from Hash.
	allPrices := make([]string, 0, len(bidMembers)+len(askMembers))
	for _, z := range bidMembers {
		if priceStr, ok := z.Member.(string); ok {
			allPrices = append(allPrices, priceStr)
		} else {
			logger.WithField("member_type", fmt.Sprintf("%T", z.Member)).Warn("Неожиданный тип члена ZSET для цены в Bids Redis.") // Используем логгер Warn
		}
	}
	for _, z := range askMembers {
		if priceStr, ok := z.Member.(string); ok {
			allPrices = append(allPrices, priceStr)
		} else {
			logger.WithField("member_type", fmt.Sprintf("%T", z.Member)).Warn("Неожиданный тип члена ZSET для цены в Asks Redis.") // Используем логгер Warn
		}
	}

	// Get volumes for all collected prices from the Hash.
	var volumeResults []interface{}
	var hmgetErr error
	if len(allPrices) > 0 {
		volumeResults, hmgetErr = s.redisClient.Client.HMGet(ctx, volumeKey, allPrices...).Result() // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx
		if hmgetErr != nil && hmgetErr != goredis.Nil {
			logger.WithError(hmgetErr).Error("Ошибка получения объемов из Redis Hash.") // Используем логгер Error
			// Логируем ошибку, но не считаем ее фатальной для получения уровней (объемы будут 0.0)
		}
	}
	// volumeResults может быть nil или пустым срезом, если HMGet вернул ошибку или запрошенные поля не найдены.

	// Map volumes to prices to form []types.PriceLevel
	volumesMap := make(map[string]string, len(allPrices))
	if len(allPrices) > 0 && len(volumeResults) > 0 {
		// Create a temporary map priceStr -> index for mapping with volumeResults
		// Ensure we don't access volumeResults out of bounds if Redis returns partial results
		for i, price := range allPrices {
			if i < len(volumeResults) { // Check bounds BEFORE access
				if vol, ok := volumeResults[i].(string); ok { // Use index `i`, not `index`
					volumesMap[price] = vol
				} else if volumeResults[i] == nil {
					// This is normal if the key was deleted from the Hash after fetching ZSET members.
					volumesMap[price] = "0"
				} else {
					logger.WithFields(logrus.Fields{
						"price":       price,
						"volume_type": fmt.Sprintf("%T", volumeResults[i]),
					}).Warn("Объем для цены не в строковом формате в Redis Hash. Предполагается 0.") // Используем логгер Warn
					volumesMap[price] = "0"
				}
			} else {
				logger.WithField("index", i).WithField("all_prices_len", len(allPrices)).WithField("volume_results_len", len(volumeResults)).Error("Несовпадение длины allPrices и volumeResults в HMGet результате.") // Используем логгер Error
				break                                                                                                                                                                                                  // Prevent panic by breaking loop if lengths don't match
			}
		}
	}

	// Form []types.PriceLevel for Bids
	parsedBids := make([]types.PriceLevel, 0, len(bidMembers))
	for _, z := range bidMembers {
		if priceStr, ok := z.Member.(string); ok {
			price, parsePriceErr := strconv.ParseFloat(priceStr, 64)
			if parsePriceErr != nil {
				logger.WithError(parsePriceErr).WithField("price_str", priceStr).Warn("Ошибка парсинга цены из ZSET Bids. Пропуск уровня.") // Используем логгер Warn
				continue                                                                                                                    // Пропускаем этот уровень
			}
			amount := 0.0 // Объем по умолчанию
			if amountStr, found := volumesMap[priceStr]; found {
				var parseAmountErr error
				amount, parseAmountErr = strconv.ParseFloat(amountStr, 64)
				if parseAmountErr != nil {
					logger.WithError(parseAmountErr).WithFields(logrus.Fields{"price_str": priceStr, "amount_str": amountStr}).Warn("Ошибка парсинга объема для цены. Предполагается 0.") // Используем логгер Warn
					amount = 0.0                                                                                                                                                          // Используем 0.0 в случае ошибки парсинга
				}
			}
			parsedBids = append(parsedBids, types.PriceLevel{Price: price, Amount: amount})
		}
	}

	// Form []types.PriceLevel for Asks
	parsedAsks := make([]types.PriceLevel, 0, len(askMembers))
	for _, z := range askMembers {
		if priceStr, ok := z.Member.(string); ok {
			price, parsePriceErr := strconv.ParseFloat(priceStr, 64)
			if parsePriceErr != nil {
				logger.WithError(parsePriceErr).WithField("price_str", priceStr).Warn("Ошибка парсинга цены из ZSET Asks. Пропуск уровня.") // Используем логгер Warn
				continue                                                                                                                    // Пропускаем этот уровень
			}
			amount := 0.0
			if amountStr, found := volumesMap[priceStr]; found {
				var parseAmountErr error
				amount, parseAmountErr = strconv.ParseFloat(amountStr, 64)
				if parseAmountErr != nil {
					logger.WithError(parseAmountErr).WithFields(logrus.Fields{"price_str": priceStr, "amount_str": amountStr}).Warn("Ошибка парсинга объема для цены. Предполагается 0.") // Используем логгер Warn
					amount = 0.0                                                                                                                                                          // Используем 0.0 в случае ошибки парсинга
				}
			}
			parsedAsks = append(parsedAsks, types.PriceLevel{Price: price, Amount: amount})
		}
	}

	logger.WithFields(logrus.Fields{
		"bids_count": len(parsedBids),
		"asks_count": len(parsedAsks),
		"last_id":    lastUpdateID,
		"last_time":  lastUpdateTime,
	}).Debug("Уровни стакана и метаданные успешно извлечены из Redis.") // Используем logrus logger Debug

	// Возвращаем извлеченные данные. Если ключи не были найдены, срезы будут пустыми, ID/Timestamp - нулевыми.
	return parsedBids, parsedAsks, lastUpdateID, lastUpdateTime, nil
}

// processBufferedBinanceDeltasLogic обрабатывает логику применения последовательных дельт из БУФЕРА
// поверх начального снимка (snapshotID).
// Эта функция НЕ работает с Scanner'ом напрямую (нет *s Receiver) и НЕ УПРАВЛЯЕТ МЬЮТЕКСОМ.
// Она получает копию буфера.
// Вызывает s.updateOrderBookInRedisZSets для применения дельт к Redis (этот метод безопасен вне s.mu).
// Возвращает флаг успешности обработки буфера и последний успешно примененный U.
// *** НОВАЯ ФУНКЦИЯ (логика буфера) - НЕ МЕТОД СТРУКТУРЫ SCANNER ***
// *** ИСПРАВЛЕНА СИГНАТУРА - принимает *Scanner для вызова updateOrderBookInRedisZSets ***
func (s *Scanner) processBufferedBinanceDeltasLogic(bufferedMsgs []*types.OrderBookMessage, snapshotID int64, logger *logrus.Entry) (bool, int64) {
	logger = logger.WithField("func", "processBufferedBinanceDeltasLogic")
	logger.WithFields(logrus.Fields{
		"buffer_size": len(bufferedMsgs),
		"snapshot_id": snapshotID, // ID снимка, относительно которого начинаем обработку
	}).Info("Начало логики обработки буфера дельт.")

	// Проверка на пустой буфер
	if len(bufferedMsgs) == 0 {
		logger.Debug("processBufferedBinanceDeltasLogic вызвана с пустым буфером. Синхронизация успешна (нет дельт во время снимка).")
		return true, snapshotID // Считаем "успехом" обработку пустого буфера, последний ID = snapshotID
	}

	// Сортируем буфер по FinalUpdateID ('u'). Это критично для правильного применения.
	// Используем Stable sort.
	sort.SliceStable(bufferedMsgs, func(i, j int) bool {
		// Проверяем на nil перед доступом к Delta (на всякий случай)
		if bufferedMsgs[i] == nil || bufferedMsgs[i].Delta == nil {
			return true
		} // nil - меньше
		if bufferedMsgs[j] == nil || bufferedMsgs[j].Delta == nil {
			return false
		} // non-nil - больше
		return bufferedMsgs[i].Delta.FinalUpdateID < bufferedMsgs[j].Delta.FinalUpdateID
	})

	lastProcessedU := snapshotID // Изначально равно ID снимка

	// Ищем ПЕРВУЮ дельту в отсортированном буфере, которая подходит после снимка.
	// Она должна удовлетворять: msg.Delta.U <= snapshotID + 1 <= msg.Delta.u.
	// Binance документация более строгая: U == snapshotID + 1.
	// Найдем первый подходящий индекс.
	firstRelevantIndex := -1
	for i, msg := range bufferedMsgs {
		if msg == nil || msg.Delta == nil {
			logger.WithField("index", i).Warn("Найден nil или nil Delta в буфере сообщений после сортировки.")
			continue // Пропускаем некорректные сообщения в буфере
		}
		// Дельта должна быть СТРОГО ПОСЛЕ снимка
		if msg.Delta.FinalUpdateID <= snapshotID {
			//logger.Tracef("Дельта из буфера (u=%d) <= snapshotID (%d), пропускаем.", msg.Delta.FinalUpdateID, snapshotID) // Too noisy trace
			continue
		}
		// Первая дельта после снимка должна начинаться СРАЗУ после снимка ID + 1.
		// Проверка диапазона U <= snapshotID+1 <= u - менее строгая, чем Binance ожидает (U == snapshotID+1).
		// Используем строгую проверку U == snapshotID + 1 для первой дельты.
		// if msg.Delta.FirstUpdateID <= snapshotID+1 && snapshotID+1 <= msg.Delta.FinalUpdateID { // Check range U <= snapshotID+1 <= u
		// Strict Binance rule: U must be exactly snapshotID + 1
		if msg.Delta.FirstUpdateID == snapshotID+1 {
			firstRelevantIndex = i                   // Нашли первую подходящую дельту.
			lastProcessedU = msg.Delta.FinalUpdateID // Обновляем lastProcessedU после применения первой дельты
			logger.WithFields(logrus.Fields{
				"index": i, "delta_U": msg.Delta.FirstUpdateID, "delta_u": msg.Delta.FinalUpdateID,
				"snapshot_id": snapshotID, "expected_U": snapshotID + 1, "new_last_u": lastProcessedU,
			}).Debug("Найдена и применена первая релевантная дельта в буфере, подходящая после снимка.")
			// Применяем эту первую дельту к Redis.
			s.updateOrderBookInRedisZSets(msg.Source, msg.MarketType, msg.Symbol, msg.Delta.Bids, msg.Delta.Asks, msg.Delta.FinalUpdateID, msg.Delta.Timestamp)

			break // Нашли, выходим из цикла поиска.
		} else {
			// Если дельта u > snapshotID, но U > snapshotID + 1, это разрыв МЕЖДУ снимком и первой дельтой.
			logger.WithFields(logrus.Fields{
				"index": i, "delta_U": msg.Delta.FirstUpdateID, "delta_u": msg.Delta.FinalUpdateID,
				"snapshot_id": snapshotID, "expected_U": snapshotID + 1,
			}).Error("Первая дельта в буфере после snapshot_id не имеет U == snapshot_id + 1. Обнаружен разрыв между снимком и буфером.")
			// Не нашли подходящую дельту для начала последовательности после снимка.
			return false, snapshotID // Разрыв. lastProcessedU остается snapshotID.
		}
	}

	// Если firstRelevantIndex == -1, значит, в буфере не было дельт, следующих непосредственно за снимком+1.
	if firstRelevantIndex == -1 {
		logger.WithField("buffer_size", len(bufferedMsgs)).Warn("В буфере нет дельт, следующих непосредственно за snapshot_id+1. Синхронизация невозможна.")
		// Это считается разрывом, т.к. мы не можем начать применять дельты.
		return false, snapshotID // Разрыв.
	}

	// Теперь применяем все последующие последовательные дельты из буфера, начиная СО ВТОРОЙ (после первой релевантной).
	appliedCount := 1 // Первая дельта уже применена

	for i := firstRelevantIndex + 1; i < len(bufferedMsgs); i++ { // Начинаем СО ВТОРОЙ дельты
		msg := bufferedMsgs[i]
		if msg == nil || msg.Delta == nil {
			logger.WithField("index", i).Warn("Найден nil или nil Delta в буфере во время обработки. Пропуск.")
			continue
		}
		delta := msg.Delta

		// Проверка последовательности: текущая дельта должна начинаться сразу после предыдущей обработанной.
		expectedU := lastProcessedU + 1
		if delta.FirstUpdateID != expectedU {
			// Обнаружен разрыв в последовательности ВНУТРИ БУФЕРА.
			logger.WithFields(logrus.Fields{
				"delta_U":          delta.FirstUpdateID,
				"delta_u":          delta.FinalUpdateID,
				"expected_U":       expectedU,
				"last_processed_u": lastProcessedU,
				"index":            i,
			}).Error("Обнаружен разрыв в последовательности дельт в буфере (U != prev_u + 1). Остановка обработки буфера.")
			// Возвращаем false, indicating sync failed. lastProcessedU is the U of the last *successfully* applied delta.
			return false, lastProcessedU
		}
		// Важно: Также убедиться, что FinalUpdateID >= FirstUpdateID.
		if delta.FinalUpdateID < delta.FirstUpdateID {
			logger.WithFields(logrus.Fields{
				"delta_U": delta.FirstUpdateID, "delta_u": delta.FinalUpdateID, "index": i,
			}).Error("Некорректные UpdateID в дельте из буфера (u < U). Остановка обработки буфера.")
			return false, lastProcessedU
		}

		// Проверка, что дельта покрывает ожидаемый ID разрыва U.
		// Этот check (delta.U == expectedU) уже достаточен.
		// Дополнительная проверка: u >= expectedU. Если u > expectedU, это тоже разрыв (пропущена целая дельта между U и u).
		// Binance дельты обычно имеют U=u-1 или U=u. Если U != u-1 и U != u, это странно, но покрывается U==expectedU.
		// Если U == expectedU, а u > expectedU, значит, дельта перепрыгнула через другие дельты.
		// Binance докум: u is the ID of the final update in the event. U is the first update.
		// The *next* delta's U should be *this* delta's u + 1.
		// So, the check should be: delta.FirstUpdateID == lastProcessedU + 1. This is what we have.

		// Если проверки пройдены, применяем дельту к Redis.
		// updateOrderBookInRedisZSets не требует мьютекса s.mu.
		s.updateOrderBookInRedisZSets(msg.Source, msg.MarketType, msg.Symbol, delta.Bids, delta.Asks, delta.FinalUpdateID, msg.Timestamp)
		lastProcessedU = delta.FinalUpdateID // Обновляем последний успешно обработанный U
		appliedCount++
		logger.WithFields(logrus.Fields{
			"delta_U":       delta.FirstUpdateID,
			"delta_u":       delta.FinalUpdateID,
			"new_last_u":    lastProcessedU,
			"applied_count": appliedCount,
		}).Trace("Применена дельта из буфера.")
	}

	// Если цикл завершился без обнаружения разрыва, буфер обработан успешно.
	logger.WithField("last_processed_u_after_buffer", lastProcessedU).WithField("applied_count", appliedCount).Info("Буфер дельт успешно обработан.")
	return true, lastProcessedU
}

// processBybitOrderBookLogic содержит специфичную логику синхронизации для Bybit.
// Она вызывается из processOrderBookMessage ПОД БЛОКИРОВКОЙ s.mu.
// Обновляет метаданные (metadata) напрямую.
// НЕ вызывает Redis операции и НЕ отправляет команды ресинка.
// Возвращает bool: true, если данные из сообщения нужно применить к Redis; false иначе.
// *** АДАПТИРОВАНА: Не управляет мьютексом, только читает metadata и msg, обновляет metadata/буфер, возвращает resyncNeeded ***
func (s *Scanner) processBybitOrderBookLogic(metadata *OrderBookSyncMetadata, msg *types.OrderBookMessage) (redisUpdateNeeded bool) {
	// Используем логгер с контекстом, добавляя update_id для отладки
	logger := s.log.WithFields(logrus.Fields{
		"event":            "process_bybit_ob_logic",
		"symbol":           metadata.Symbol,
		"market_type":      metadata.MarketType,
		"msg_type":         msg.Type,
		"update_id":        msg.Delta.FinalUpdateID, // 'u' для snapshot/delta
		"current_bybit_id": metadata.BybitUpdateID,
		"current_status":   metadata.CurrentStatus,
		"bybit_depth":      metadata.BybitDepth,
	})

	finalUpdateID := msg.Delta.FinalUpdateID // 'u'

	// redisUpdateNeeded по умолчанию false
	redisUpdateNeeded = false

	switch msg.Type {
	case "snapshot": // Первое сообщение после подписки на orderbook.{depth}.{symbol} Bybit V5
		if msg.Delta == nil {
			logger.Error("Получен WS SNAPSHOT, но Delta == nil. Игнорирование.")
			metadata.BybitUpdateID = 0
			metadata.CurrentStatus = "WS Snapshot Error"
			// Сигнализируем о необходимости ресинка, т.к. снимок некорректен.
			// redisUpdateNeeded = false // Не применяем неполный снимок
			return false // Return false, processOrderBookMessage will see status and send resync.
		}
		snapshotID := finalUpdateID // Update ID из Bybit snapshot ('u').

		// Извлекаем глубину из топика Bybit V5 (orderbook.{depth}.{symbol}) используя RawData
		depthStr := extractDepthFromBybitTopic(msg.RawData) // extractDepthFromBybitTopic определен ниже
		var depth int
		if depthStr != "" {
			d, err := strconv.Atoi(depthStr)
			if err == nil {
				depth = d
			}
		}
		if depth > 0 {
			// Проверяем, соответствует ли глубина снимка той, что уже записана в метаданных,
			// если метаданные уже существуют (например, после переподписки).
			// Если не соответствует, это может указывать на проблему или изменение подписки.
			if metadata.BybitDepth != 0 && metadata.BybitDepth != depth {
				logger.WithFields(logrus.Fields{
					"snapshot_depth": depth, "metadata_depth": metadata.BybitDepth,
				}).Warn("Глубина снимка отличается от глубины в метаданных. Обновление метаданных.")
			}
			metadata.BybitDepth = depth // Сохраняем/обновляем глубину в метаданные
			logger.WithField("depth", depth).Debug("Определена глубина из топика снимка. Сохранение/обновление в метаданных.")
		} else {
			logger.Warn("Не удалось определить глубину из топика снимка. BybitDepth в метаданных может быть некорректна.")
			// TODO: Возможно, инициировать ресинк, если не удалось определить глубину из снимка?
			// redisUpdateNeeded = false; return false // Сигнализировать о ресинке, если глубина не определена?
		}

		logger.WithFields(logrus.Fields{
			"snapshot_id":    snapshotID,
			"bids_count":     len(msg.Delta.Bids),
			"asks_count":     len(msg.Delta.Asks),
			"current_status": metadata.CurrentStatus,
			"prev_bybit_id":  metadata.BybitUpdateID,
		}).Info("Получен WS SNAPSHOT. Инициализация стакана.")

		// Обновляем метаданные для снимка.
		metadata.BybitUpdateID = snapshotID
		metadata.LastUpdateTime = time.Now() // Обновляем время последнего обновления.
		metadata.CurrentStatus = "Synced"    // После WS snapshot считаем себя синхронизированными.

		// Применение к Redis ZSETs будет вызвано после возврата из этой функции.
		// Вызывающий processOrderBookMessage сделает s.updateOrderBookInRedisZSets.
		redisUpdateNeeded = true // Сигнализируем, что снимок нужно применить к Redis.

		logger.WithFields(logrus.Fields{
			"new_bybit_id": metadata.BybitUpdateID,
			"new_status":   metadata.CurrentStatus,
		}).Info("Обработка WS SNAPSHOT завершена. Ожидание последовательных дельт.")
		return redisUpdateNeeded // Возвращает true, синхронизация успешна после снимка.

	case "delta": // Последующие сообщения после снимка
		if msg.Delta == nil {
			logger.Warn("Получен WS DELTA, но Delta == nil. Игнорирование.")
			return false // Не применяем к Redis
		}
		deltaU := finalUpdateID // Update ID из Bybit delta ('u').

		logger = logger.WithFields(logrus.Fields{"delta_u": deltaU, "current_bybit_id": metadata.BybitUpdateID, "current_status": metadata.CurrentStatus}) // Добавляем поля для этой ветки

		// Если BybitUpdateID еще не установлен (0) ИЛИ текущий статус не "Synced" (т.е., мы не обработали снимок)...
		if metadata.BybitUpdateID == 0 || metadata.CurrentStatus != "Synced" {
			// Игнорируем дельту, пока не получим и не обработаем WS snapshot.
			// TODO: Должны ли мы буферизовать дельты для Bybit? Bybit V5 отправляет полный снимок после реподписки.
			// Если да, нужно добавить буфер как в Binance логике. Если нет, просто игнорируем до снимка.
			logger.Debug("Не в статусе 'Synced'. Игнорирование дельты Bybit. Ожидание WS snapshot.")
			return false // Игнорируем дельту, не применяем к Redis.
		}

		// Если delta UpdateID меньше или равен UpdateID последнего обработанного сообщения (snapshot или delta), игнорируем.
		if deltaU <= metadata.BybitUpdateID {
			// logger.Trace("Игнорирование старой дельты.") // Очень шумно
			return false // Игнорируем старое сообщение, не применяем к Redis.
		}

		// Проверяем строгую последовательность: deltaU == metadata.BybitUpdateID + 1
		expectedU := metadata.BybitUpdateID + 1

		logger = logger.WithField("expected_u", expectedU) // Добавляем поле

		if deltaU == expectedU {
			// Последовательность сохранена. Обновляем метаданные.
			// Применение к Redis ZSETs будет вызвано после возврата из этой функции.
			metadata.BybitUpdateID = deltaU      // Обновляем BybitUpdateID на 'u' текущей дельты.
			metadata.LastUpdateTime = time.Now() // Обновляем время.
			// Статус остается Synced.
			logger.WithFields(logrus.Fields{"delta_u": deltaU, "new_bybit_id": metadata.BybitUpdateID}).Trace("Последовательная дельта Bybit. Метаданные обновлены.")
			redisUpdateNeeded = true // Сигнализируем, что дельту нужно применить к Redis.
			return redisUpdateNeeded

		} else if deltaU > expectedU {
			// Пропущена одна или несколько дельт. Обнаружен разрыв. Требуется ресинхронизация (через реподписку).
			logger.Error("Обнаружен разрыв в последовательности дельт Bybit! Инициирование ресинхронизации (реподписки)!")
			// Сбрасываем метаданные для инициирования нового цикла синхронизации.
			metadata.BybitUpdateID = 0                         // Сброс ID на 0 при ожидании нового снимка после реподписки.
			metadata.CurrentStatus = "Gap Detected, Resyncing" // Обновляем статус.
			// TODO: Нужна ли буферизация для Bybit? Bybit V5 отправляет полный снимок после реподписки. Вероятно, нет.
			// Если буферизация не нужна, оставляем BinanceDeltaBuffer пустым для Bybit.

			redisUpdateNeeded = false // Не применяем текущую дельту к Redis.
			// Ресинк будет инициирован processOrderBookMessage после разблокировки s.mu.
			return redisUpdateNeeded // Return false, processOrderBookMessage will see status and send resync.

		} else { // deltaU < expectedU - должно быть отфильтровано проверкой deltaU <= metadata.BybitUpdateID
			// Это не должно происходить при правильной фильтрации старых дельт.
			logger.WithField("check", "old_delta_filter").Warn("Неожиданная дельта Bybit (u > current_bybit_id, но u < expected_u). Возможно, ошибка фильтрации старых дельт или биржи. Игнорирование.")
			return false // Игнорируем.
		}

	case "snapshot_rest":
		// Scanner ожидает WS snapshot для Bybit, не REST snapshot.
		logger.Warn("Получен REST snapshot через OrderBookChan для Bybit. В текущей логике предпочитается WS синхронизация. Игнорирование.")
		metadata.CurrentStatus = "Warning: REST Snapshot in OB Chan (Bybit)"
		return false // Не применяем к Redis

	case "bookTicker":
		// BookTicker сообщения обрабатываются processBBO.
		logger.Warn("Получено сообщение типа 'bookTicker' через OrderBookChan для Bybit. Этого не должно происходить. Игнорирование.")
		metadata.CurrentStatus = "Warning: BookTicker in OB Chan (Bybit)"
		return false // Не применяем к Redis

	default:
		logger.WithField("msg_type", msg.Type).Error("Получено сообщение с неизвестным типом для Bybit. Игнорирование.")
		metadata.CurrentStatus = fmt.Sprintf("Unknown Message Type: %s", msg.Type)
		return false // Не применяем к Redis
	}
}

func extractDepthFromBybitTopic(rawData json.RawMessage) string {
	var msgData struct {
		Topic string `json:"topic"`
	}
	// Используем logrus для логирования ошибок парсинга в этой функции
	logger := logrus.WithField("func", "extractDepthFromBybitTopic") // Используем logrus напрямую здесь, т.к. это служебная функция парсинга

	if err := json.Unmarshal(rawData, &msgData); err == nil && msgData.Topic != "" {
		// Expected format: orderbook.{depth}.{symbol} or public.orderbook.depth.{symbol}
		parts := strings.Split(msgData.Topic, ".")
		// Check for both orderbook.{depth}.{symbol} (3 parts total for depth) and public.orderbook.depth.{symbol} (4 parts total for depth)
		if len(parts) == 3 && parts[0] == "orderbook" { // Handles orderbook.{depth}.{symbol}
			// Check if the second part is a number (the depth)
			if _, err := strconv.Atoi(parts[1]); err == nil {
				logger.WithField("topic", msgData.Topic).WithField("depth", parts[1]).Trace("Извлечена глубина из топика (формат orderbook.{depth}.{symbol}).")
				return parts[1] // Return the depth part (as a string)
			}
		} else if len(parts) == 4 && parts[0] == "public" && parts[1] == "orderbook" { // Handles public.orderbook.depth.{symbol}
			// Check if the third part is a number (the depth)
			if _, err := strconv.Atoi(parts[2]); err == nil {
				logger.WithField("topic", msgData.Topic).WithField("depth", parts[2]).Trace("Извлечена глубина из топика (формат public.orderbook.depth.{symbol}).")
				return parts[2] // Return the depth part (as a string)
			}
		}
		// If topic was found but didn't match expected patterns
		logger.WithField("topic", msgData.Topic).Warn("RawData contains 'topic' field, but format doesn't match expected Bybit depth topic patterns.")

	} else if err != nil {
		logger.WithError(err).WithField("raw_data", string(rawData)).Warn("Ошибка Unmarshal RawData для извлечения топика Bybit.")
	} else if msgData.Topic == "" {
		logger.WithField("raw_data", string(rawData)).Trace("Поле 'topic' не найдено или пусто в RawData Bybit.") // Trace, т.к. это может быть не orderbook сообщение
	}

	// Return empty string if parsing fails or topic format is unexpected
	return ""
}

// sendResyncCommand отправляет команду ресинхронизации в соответствующий канал клиента.
// Запускается в отдельной горутине для избежания блокировки. Добавляет горутину в scannerWg.
// Этот метод вызывается из других методов Scanner'а (например, processOrderBookMessage
// при обнаружении разрыва, или из processBufferedBinanceDeltasWorker).
// *** НОВЫЙ МЕТОД ***
func (s *Scanner) sendResyncCommand(source string, marketType types.MarketType, symbol string, cmdType types.ResyncCommandType, logger *logrus.Entry) {
	// Логгер для этой функции
	cmdLogger := logger.WithFields(logrus.Fields{
		"func":        "sendResyncCommand",
		"source":      source,
		"market_type": marketType,
		"symbol":      symbol, // Symbol is uppercase here
		"cmd_type":    cmdType,
	})
	cmdLogger.Debug("Подготовка к отправке команды ресинка клиенту.")

	// Find the correct client channel key
	clientKey := fmt.Sprintf("%s:%s", source, marketType)
	resyncChan, ok := s.resyncCmdChannels[clientKey] // resyncCmdChannels is map[string]chan<- *types.ResyncCommand
	if !ok {
		cmdLogger.WithField("client_key", clientKey).Error("Канал ресинка для клиента не найден в мапе. Невозможно отправить команду.")
		// TODO: Возможно, обновить статус метаданных, что отправка команды ресинка провалилась?
		// Для этого потребуется s.mu.Lock()/Unlock() ЗДЕСЬ.
		return // Cannot send command, exit.
	}

	// Create the command structure
	resyncCmd := &types.ResyncCommand{
		Source:     source,
		MarketType: marketType,
		Symbol:     symbol, // Symbol is uppercase
		Type:       cmdType,
		// Depth, etc. can be added if needed for specific command types (e.g., BybitResubscribe requires depth)
		// For BinanceSnapshot, Depth is not needed in the command itself, the client will fetch a default/configured depth.
	}
	// Add depth for BybitResubscribe if available in metadata
	if cmdType == types.ResyncTypeBybitResubscribe {
		s.mu.Lock()
		metadataKey := fmt.Sprintf("%s:%s:%s", source, marketType, symbol)
		if md, found := s.orderBookMetadata[metadataKey]; found && md.BybitDepth > 0 {
			resyncCmd.Depth = strconv.Itoa(md.BybitDepth) // Add depth as string
			cmdLogger.WithField("depth", resyncCmd.Depth).Debug("Добавлена глубина к команде BybitResubscribe.")
		} else {
			cmdLogger.Warn("Глубина не найдена в метаданных для команды BybitResubscribe. Команда может быть неполной.")
		}
		s.mu.Unlock()
	}

	// Launch a goroutine to send the command to the client channel.
	// Add this goroutine to Scanner's WaitGroup.
	s.scannerWg.Add(1) // Add to scanner's WG

	go func(cmd *types.ResyncCommand, targetChan chan<- *types.ResyncCommand, cmdSenderLogger *logrus.Entry) {
		defer s.scannerWg.Done() // Done on scanner's WG

		cmdSenderLogger.Debug("Попытка отправки команды ресинка в канал клиента...")

		// Create a context with timeout for sending to the channel.
		sendCtx, cancelSend := context.WithTimeout(s.ctx, 5*time.Second) // Use Scanner's context
		defer cancelSend()

		select {
		case targetChan <- cmd: // Send to the specific client channel (chan<-)
			cmdSenderLogger.Info("Команда ресинка успешно отправлена в канал клиента.")
			// TODO: Notify web interface or metrics?

		case <-sendCtx.Done(): // Listen for send context cancellation (timeout or Scanner stop)
			cmdSenderLogger.Warn("Контекст отменен при попытке отправить команду ресинка. Отправка пропущена.")
			// TODO: Update metadata status that sending failed? Requires s.mu.Lock()/Unlock().
			// Need metadataKey here, difficult inside this anonymous goroutine unless passed.
			// For now, just log.
		}
		cmdSenderLogger.Debug("Горутина отправки команды ресинка завершена.")
	}(resyncCmd, resyncChan, cmdLogger) // Pass command, channel, and logger

	cmdLogger.Debug("Горутина отправки команды ресинка запущена.")
}

// getOrderBookZSetKeys возвращает имена ключей Redis ZSET, Hash и String для стакана.
// Включает source, marketType и symbol.
// *** НЕ ИЗМЕНЕН ***
func getOrderBookZSetKeys(source string, marketType types.MarketType, symbol string) (bidsKey, asksKey, volumeKey, infoKey string) {
	// Use uppercase symbol for key consistency
	baseKey := fmt.Sprintf("orderbook:%s:%s:%s", source, marketType, strings.ToUpper(symbol)) // *** ИЗМЕНЕН ФОРМАТ КЛЮЧА ***
	bidsKey = baseKey + ":bids"
	asksKey = baseKey + ":asks"
	volumeKey = baseKey + ":volume"
	infoKey = baseKey + ":info" // Key for storing metadata (ID, Timestamp, etc.)
	return
}

// updateOrderBookInRedisZSets обновляет уровни стакана в Redis ZSETs и Hash из списка PriceLevel.
// Также сохраняет метаданные стакана (FinalUpdateID и Timestamp) в отдельный ключ infoKey (String с JSON).
// Этот метод вызывается ВНЕ основного мьютекса Scanner'а (s.mu). Использует контекст Scanner'а (s.ctx).
// *** НЕ ИЗМЕНЕНА (кроме использования s.ctx) ***
func (s *Scanner) updateOrderBookInRedisZSets(source string, marketType types.MarketType, symbol string, bidsToUpdate, asksToUpdate []types.PriceLevel, finalUpdateID int64, timestamp time.Time) {
	upperSymbol := strings.ToUpper(symbol)
	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":            "updateOrderBookInRedisZSets",
		"source":          source,
		"market_type":     marketType,
		"symbol":          upperSymbol,
		"bids_to_update":  len(bidsToUpdate),
		"asks_to_update":  len(asksToUpdate),
		"final_update_id": finalUpdateID,
		"timestamp":       timestamp,
	})

	if len(bidsToUpdate) == 0 && len(asksToUpdate) == 0 && finalUpdateID == 0 {
		logger.Trace("Нет данных или метаданных для обновления Redis. Пропуск.")
		return
	}
	// Check context cancellation before starting Redis operations
	select {
	case <-s.ctx.Done():
		logger.Warn("Context cancelled during updateOrderBookInRedisZSets. Aborting Redis operations.")
		return // Exit if context cancelled
	default:
		// Continue
	}

	// Get keys using the helper function that includes market type.
	bidsKey, asksKey, volumeKey, infoKey := getOrderBookZSetKeys(source, marketType, upperSymbol) // *** ИСПОЛЬЗУЕТ НОВУЮ ФУНКЦИЮ ***

	pipe := s.redisClient.Client.Pipeline()

	// --- Updating order book levels (Bids, Asks, Volume) ---
	if len(bidsToUpdate) > 0 {
		zAddsBids := make([]*goredis.Z, 0, len(bidsToUpdate))
		hSetBids := make(map[string]interface{}, len(bidsToUpdate))
		zRemMembersBids := make([]interface{}, 0) // Prices to remove from ZSET
		hDelKeysBids := make([]string, 0)         // Prices to delete from Hash

		for _, level := range bidsToUpdate {
			priceStr := strconv.FormatFloat(level.Price, 'f', -1, 64)

			if level.Amount > 0 {
				zAddsBids = append(zAddsBids, &goredis.Z{Score: level.Price, Member: priceStr}) // Add/Update price in ZSET
				hSetBids[priceStr] = strconv.FormatFloat(level.Amount, 'f', -1, 64)             // Set/Update volume in Hash
			} else {
				// If Amount == 0, remove the level
				zRemMembersBids = append(zRemMembersBids, priceStr) // Remove price from ZSET
				hDelKeysBids = append(hDelKeysBids, priceStr)       // Delete price from Hash
			}
		}

		if len(zAddsBids) > 0 {
			pipe.ZAdd(s.ctx, bidsKey, zAddsBids...) // *** ИСПОЛЬЗУЕТ bidsKey и s.ctx ***
		}
		if len(hSetBids) > 0 {
			pipe.HSet(s.ctx, volumeKey, hSetBids) // *** ИСПОЛЬЗУЕТ volumeKey и s.ctx ***
		}
		if len(zRemMembersBids) > 0 {
			pipe.ZRem(s.ctx, bidsKey, zRemMembersBids...) // *** ИСПОЛЬЗУЕТ bidsKey и s.ctx ***
		}
		if len(hDelKeysBids) > 0 {
			pipe.HDel(s.ctx, volumeKey, hDelKeysBids...) // *** ИСПОЛЬЗУЕТ volumeKey и s.ctx ***
		}
	}

	if len(asksToUpdate) > 0 {
		zAddsAsks := make([]*goredis.Z, 0, len(asksToUpdate))
		hSetAsks := make(map[string]interface{}, len(asksToUpdate))
		zRemMembersAsks := make([]interface{}, 0)
		hDelKeysAsks := make([]string, 0)

		for _, level := range asksToUpdate {
			priceStr := strconv.FormatFloat(level.Price, 'f', -1, 64)

			if level.Amount > 0 {
				zAddsAsks = append(zAddsAsks, &goredis.Z{Score: level.Price, Member: priceStr}) // Add/Update price in ZSET
				hSetAsks[priceStr] = strconv.FormatFloat(level.Amount, 'f', -1, 64)             // Set/Update volume in Hash
			} else {
				// If Amount == 0, remove the level
				zRemMembersAsks = append(zRemMembersAsks, priceStr) // Remove price from ZSET
				hDelKeysAsks = append(hDelKeysAsks, priceStr)       // Delete price from Hash
			}
		}

		if len(zAddsAsks) > 0 {
			pipe.ZAdd(s.ctx, asksKey, zAddsAsks...) // *** ИСПОЛЬЗУЕТ asksKey и s.ctx ***
		}
		if len(hSetAsks) > 0 {
			pipe.HSet(s.ctx, volumeKey, hSetAsks) // *** ИСПОЛЬЗУЕТ volumeKey и s.ctx ***
		}
		if len(zRemMembersAsks) > 0 {
			pipe.ZRem(s.ctx, asksKey, zRemMembersAsks...) // *** ИСПОЛЬЗУЕТ asksKey и s.ctx ***
		}
		if len(hDelKeysAsks) > 0 {
			pipe.HDel(s.ctx, volumeKey, hDelKeysAsks...) // *** ИСПОЛЬЗУЕТ volumeKey и s.ctx ***
		}
	}

	// *** Saving order book metadata (FinalUpdateID, Timestamp) ***
	// Use a String with JSON for infoKey.
	infoData := OrderBookInfo{ // >>> Используем структуру с уровня пакета - FIXED
		FinalUpdateID: finalUpdateID,
		Timestamp:     timestamp,
	}
	infoJson, err := json.Marshal(infoData)
	if err != nil {
		logger.WithError(err).Error("Ошибка маршалинга OrderBook info в JSON. Метаданные не будут сохранены.")
		// Не считаем это фатальной ошибкой, просто не сохраним метаданные в Redis
	} else {
		// Save JSON metadata to infoKey
		pipe.Set(s.ctx, infoKey, infoJson, 0) // 0 - нет TTL // *** ИСПОЛЬЗУЕТ infoKey и s.ctx ***
	}

	// Execute the pipeline
	_, err = pipe.Exec(s.ctx) // *** ИСПОЛЬЗУЕТ s.ctx ***
	if err != nil {
		logger.WithError(err).Error("Ошибка выполнения Redis pipeline для обновления стакана.")
	} else {
		// Лог об успешном применении может быть очень шумным.
		// Логируем на уровне Trace или Debug.
		logger.WithFields(logrus.Fields{
			"keys": []string{bidsKey, asksKey, volumeKey, infoKey},
		}).Trace("Уровни стакана и метаданные успешно применены в Redis.") // Уровень Trace
	}
}

func (s *Scanner) processBBO(bbo *types.BBO) {
	// Используем логгер с контекстом сообщения
	logger := s.log.WithFields(logrus.Fields{
		"event":       "process_bbo",
		"source":      bbo.Source,
		"market_type": bbo.MarketType,
		"symbol":      bbo.Symbol,
	})

	if bbo == nil {
		logger.Warn("Получено nil BBO message. Игнорирование.")
		return
	}

	// Check if we are tracking this symbol (uppercase)
	upperSymbol := strings.ToUpper(bbo.Symbol)
	// Проверка наличия в глобальном списке отслеживаемых символов
	// s.trackedSymbols - это мапа, инициализированная в NewScanner и неизменяемая после.
	if _, ok := s.trackedSymbols[upperSymbol]; !ok {
		logger.WithField("symbol", upperSymbol).Debug("Игнорирование BBO для неотслеживаемого символа.") // Debug, т.к. это нормальная ситуация
		return
	}

	// Store BBO in Redis. SaveBBO method serializes BBO to JSON or Binary.
	// Important: Use MarketType in Redis key
	// Ensure Symbol in BBO struct is uppercase before saving for key consistency
	bbo.Symbol = upperSymbol // Убедимся, что символ в верхнем регистре в самой структуре BBO

	// s.redisClient.SaveBBO должен принимать контекст и структуру BBO
	err := s.redisClient.SaveBBO(s.ctx, bbo) // Используем контекст Scanner'а s.ctx

	if err != nil {
		// Логируем ошибку с контекстом
		logger.WithError(err).Error("Ошибка сохранения BBO в Redis.")
	} else {
		// Лог об успешном сохранении BBO может быть очень шумным на высокой частоте.
		// Логируем его на уровне Trace или Debug.
		logger.WithFields(logrus.Fields{
			"bid_price":  bbo.BestBid,
			"bid_amount": bbo.BestBidQuantity,
			"ask_price":  bbo.BestAsk,
			"ask_amount": bbo.BestAskQuantity,
		}).Trace("BBO успешно сохранен в Redis.") // Уровень Trace
	}
}

// clearOrderBookInRedisZSets очищает все ключи стакана для данной пары/биржи в Redis.
// Использует контекст Scanner'а (s.ctx).
// *** НЕ ИЗМЕНЕНА ***
func (s *Scanner) clearOrderBookInRedisZSets(source string, marketType types.MarketType, symbol string) { // *** ДОБАВЛЕНО marketType ***
	upperSymbol := strings.ToUpper(symbol)
	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":        "clearOrderBookInRedisZSets",
		"source":      source,
		"market_type": marketType,
		"symbol":      upperSymbol,
	})

	// Get keys using the new helper function that includes market type.
	bidsKey, asksKey, volumeKey, infoKey := getOrderBookZSetKeys(source, marketType, upperSymbol) // *** ИСПОЛЬЗУЕТ НОВУЮ ФУНКЦИЮ ***
	keysToDelete := []string{bidsKey, asksKey, volumeKey, infoKey}

	logger.WithField("keys", keysToDelete).Info("Очистка стакана в Redis (удаление ключей).")

	// Delete all 4 keys using Scanner's context
	deletedCount, err := s.redisClient.Client.Del(s.ctx, keysToDelete...).Result() // *** ИСПОЛЬЗУЕТ s.ctx ***
	if err != nil {
		logger.WithError(err).Error("Ошибка очистки стакана в Redis.")
	} else {
		logger.WithField("deleted_count", deletedCount).Info("Очистка стакана в Redis завершена. Ключей удалено.")
	}
}

// GetBBO извлекает BBO для данной пары Биржа:ТипРынка:Символ из Redis.
// Использует контекст.
// *** НЕ ИЗМЕНЕНА ***
func (s *Scanner) GetBBO(ctx context.Context, source string, marketType types.MarketType, symbol string) (*types.BBO, error) {
	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":        "GetBBO",
		"source":      source,
		"market_type": marketType,
		"symbol":      strings.ToUpper(symbol),
	})
	logger.Trace("Извлечение BBO из Redis.") // Уровень Trace, т.к. может вызываться часто

	// Get key using the helper function that includes market type.
	key := fmt.Sprintf("bbo:%s:%s:%s", source, marketType, strings.ToUpper(symbol)) // *** ИЗМЕНЕН ФОРМАТ КЛЮЧА ***

	// Используем переданный контекст ctx для операции Get
	binaryData, err := s.redisClient.Client.Get(ctx, key).Bytes() // <<< ИСПОЛЬЗУЕМ ПЕРЕДАННЫЙ ctx
	if err != nil {
		if err == goredis.Nil {
			// BBO не найден, это не ошибка. Возвращаем nil.
			logger.Trace("BBO не найден в Redis.") // Trace
			return nil, nil
		}
		// Другие ошибки Redis
		logger.WithError(err).Error("Ошибка получения BBO из Redis.")
		return nil, fmt.Errorf("Scanner: Error getting BBO from Redis for %s:%s:%s: %w", source, marketType, strings.ToUpper(symbol), err)
	}

	var bbo types.BBO
	// Используем UnmarshalBinary из types.BBO
	err = bbo.UnmarshalBinary(binaryData)
	if err != nil {
		// Ошибка десериализации данных
		logger.WithError(err).WithField("key", key).WithField("data", string(binaryData)).Error("Ошибка десериализации BBO из Redis.")
		return nil, fmt.Errorf("Scanner: Error unmarshalling BBO from Redis for %s:%s:%s: %w, data: %s", source, marketType, strings.ToUpper(symbol), err, string(binaryData))
	}

	logger.Trace("BBO успешно извлечен из Redis.") // Trace

	return &bbo, nil
}

// GetBBOsForSymbol извлекает BBO для данного символа со всех указанных рынков из Redis.
// Использует контекст.
// *** НЕ ИЗМЕНЕНА ***
func (s *Scanner) GetBBOsForSymbol(ctx context.Context, symbol string, exchanges []string) (map[string]types.BBO, error) {
	upperSymbol := strings.ToUpper(symbol)
	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":              "GetBBOsForSymbol",
		"symbol":            upperSymbol,
		"requested_markets": exchanges,
	})
	logger.Debug("Запрос BBO для символа со всех указанных рынков.")

	bbos := make(map[string]types.BBO)
	// Итерируем по списку строк "source:market_type"
	for _, exchangeMarket := range exchanges {
		parts := strings.Split(exchangeMarket, ":")
		if len(parts) != 2 {
			logger.WithField("exchange_market_str", exchangeMarket).Warn("Некорректный формат 'биржа:тип_рынка' для запроса BBO. Пропуск.")
			continue
		}
		source := parts[0]
		marketType := types.MarketType(parts[1]) // Преобразуем строку в MarketType

		// Вызываем обновленный метод GetBBO с source и market type.
		bbo, err := s.GetBBO(ctx, source, marketType, upperSymbol) // *** ВКЛЮЧАЕМ marketType *** // GetBBO обрабатывает ошибки "не найдено" внутри себя
		if err != nil {
			// Другие ошибки логируются в GetBBO. Просто пропускаем эту комбинацию рынка.
			logger.WithError(err).WithFields(logrus.Fields{"source": source, "market_type": marketType}).Error("Ошибка при получении BBO из Redis для рынка. Пропуск.")
			continue
		}
		if bbo != nil {
			// BBO найден и успешно извлечен, добавляем его в мапу результатов.
			bbos[exchangeMarket] = *bbo // Сохраняем копию структуры BBO в мапе, используем "source:market_type" как ключ
		} else {
			// BBO не найден в Redis для этой пары.
			logger.WithFields(logrus.Fields{"source": source, "market_type": marketType}).Debug("BBO не найден в Redis для рынка.")
		}
	}

	logger.WithField("retrieved_count", len(bbos)).Debug("Извлечение BBO для символа со всех указанных рынков завершено.")

	// Возвращаем собранную мапу. Она может быть пустой, если ни одного BBO не было извлечено.
	return bbos, nil
}

// GetSyncMetadata возвращает метаданные синхронизации для биржи и символа из in-memory карты Scanner'а.
// Этот метод безопасен для конкурентного доступа.
// *** НЕ ИЗМЕНЕНА ***
func (s *Scanner) GetSyncMetadata(source string, marketType types.MarketType, symbol string) (*OrderBookSyncMetadata, bool) {
	s.mu.Lock()         // Блокируем для безопасного доступа к orderBookMetadata map
	defer s.mu.Unlock() // Гарантируем разблокировку

	// Используем новый формат ключа: source:market_type:symbol
	key := fmt.Sprintf("%s:%s:%s", source, marketType, strings.ToUpper(symbol)) // *** ИЗМЕНЕН ФОРМАТ КЛЮЧА ***
	metadata, ok := s.orderBookMetadata[key]

	// Используем логгер с контекстом для этого метода
	logger := s.log.WithFields(logrus.Fields{
		"func":         "GetSyncMetadata",
		"source":       source,
		"market_type":  marketType,
		"symbol":       strings.ToUpper(symbol), // Логируем символ в верхнем регистре
		"metadata_key": key,
		"found":        ok,
	})
	logger.Trace("Извлечение метаданных синхронизации.")

	// Return a copy of the metadata to prevent external modification under our lock
	if ok {
		metadataCopy := *metadata // Создаем копию структуры
		// Ensure buffer is also copied if it's slice/map (it is a slice)
		metadataCopy.BinanceDeltaBuffer = make([]*types.OrderBookMessage, len(metadata.BinanceDeltaBuffer))
		copy(metadataCopy.BinanceDeltaBuffer, metadata.BinanceDeltaBuffer)

		logger.Trace("Метаданные найдены.")
		return &metadataCopy, true // Возвращаем указатель на КОПИЮ
	}
	// Если метаданные не найдены, возвращаем nil и false
	logger.Trace("Метаданные не найдены.")
	return nil, false
}

// GetTrackedSymbols возвращает мапу символов, которые отслеживает Scanner.
// Ключ: Символ (uppercase), Значение: bool (true)
// *** НЕ ИЗМЕНЕНА ***
func (s *Scanner) GetTrackedSymbols() map[string]bool {
	// Предполагается, что мапа trackedSymbols модифицируется только в NewScanner,
	// поэтому безопасна для чтения без мьютекса после инициализации.
	return s.trackedSymbols
}

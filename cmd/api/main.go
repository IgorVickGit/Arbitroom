// cmd/api/main.go
package main

import (
	"context" // Работа с контекстом
	"fmt"     // Для форматирования строк (особенно ключей бирж/рынков)

	// "log"       // >>> Удаляем стандартное логирование
	"os"        // Работа с ОС
	"os/signal" // Обработка сигналов ОС

	"sync"    // Примитивы синхронизации (WaitGroup)
	"syscall" // Системные вызовы

	"time" // Работа со временем

	"github.com/sirupsen/logrus" // Импортируем logrus для использования logrus.Fields и Entry

	// Импортируем ваши пакеты (убедитесь, что пути правильные)
	"arbitrage-engine/internal/arbitrage"
	"arbitrage-engine/internal/config"
	"arbitrage-engine/internal/database"
	"arbitrage-engine/internal/exchanges/binance" // Обновлен конструктор для приема *logrus.Entry и ExchangeChannels
	"arbitrage-engine/internal/exchanges/bybit"   // Обновлен конструктор для приема *logrus.Entry и ExchangeChannels
	"arbitrage-engine/internal/logger"            // >>> Импортируем наш централизованный логгер
	"arbitrage-engine/internal/rate_limiter"
	"arbitrage-engine/internal/redis"
	"arbitrage-engine/internal/scanner"      // Обновлен конструктор для приема *logrus.Entry, мап каналов chan<-, и списка символов
	"arbitrage-engine/internal/subscription" // Обновлен конструктор для приема мапы каналов chan<-
	"arbitrage-engine/internal/types"        // Обновлены типы с MarketType
	"arbitrage-engine/internal/web"          // Обновлен конструктор для приема *logrus.Entry
)

func main() {
	// 1. Загрузка конфигурации
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Ошибка при загрузке конфигурации: %v\n", err)
		os.Exit(1)
	}

	// 2. Инициализация логгера
	err = logger.InitLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Ошибка при инициализации логирования: %v\n", err)
		os.Exit(1)
	}

	logger.Log.Info("Приложение arbitrage-engine запущено.")
	logger.Log.WithFields(logrus.Fields{
		"log_level":       cfg.Log.Level,
		"log_file_path":   cfg.Log.FilePath,
		"redis_addr":      cfg.Redis.Addr,
		"db_name":         cfg.Database.DBName,
		"binance_enabled": cfg.Exchanges.Binance.Enabled,
		"bybit_enabled":   cfg.Exchanges.Bybit.Enabled,
	}).Info("Конфигурация успешно загружена и применена.")

	// 3. Управление контекстом и WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// --- ОБЪЯВЛЕНИЯ ПЕРЕМЕННЫХ ВНУТРИ main ---
	// Эти переменные должны быть объявлены здесь
	var binanceRestLimiter *rate_limiter.Limiter // Объявлен здесь
	var bybitRestLimiter *rate_limiter.Limiter   // Объявлен здесь
	// Каналы обмена данными (общие)
	var orderBookMsgChan chan *types.OrderBookMessage
	var bboChan chan *types.BBO
	// Каналы команд (специфичные для клиентов), храним исходные двунаправленные каналы
	var resyncCmdChannels map[string]chan *types.ResyncCommand
	var subscriptionCmdChannels map[string]chan *types.SubscriptionCommand
	// Список отслеживаемых символов (для Scanner'а)
	trackedSymbols := make(map[string]struct{}) // Используем мапу для уникальности символов
	// --- КОНЕЦ ОБЪЯВЛЕНИЙ ПЕРЕМЕННЫХ ---

	// 4. Инициализация компонентов инфраструктуры

	// Инициализируем отдельные лимитеры REST для каждой биржи
	logger.Log.Infof("Инициализация Rate Limiter'а для Binance REST (%d/мин)...", cfg.RateLimits.BinanceRestPerMin)
	// Предполагаем, что NewLimiter(ctx, ...) создает лимитер, управляемый контекстом.
	binanceRestLimiter = rate_limiter.NewLimiter(ctx, cfg.RateLimits.BinanceRestPerMin, time.Minute) // Объявлен и инициализирован здесь
	// Если ваш rate_limiter.Limiter имеет метод Run, запустите его здесь и добавьте в wg.
	// Если он не имеет Run и блокируется только в Wait, то запускать ничего не нужно.
	// Предполагаю, что Run не нужен для *этого* типа Limiter.
	defer binanceRestLimiter.Stop() // Закрываем Binance REST лимитер

	logger.Log.Infof("Инициализация Rate Limiter'а для Bybit REST (%d/мин)...", cfg.RateLimits.BybitRestPerMin)
	bybitRestLimiter = rate_limiter.NewLimiter(ctx, cfg.RateLimits.BybitRestPerMin, time.Minute) // Объявлен и инициализирован здесь
	// Предполагаю, что Run не нужен для *этого* типа Limiter.
	defer bybitRestLimiter.Stop() // Закрываем Bybit REST лимитер

	logger.Log.Info("Отдельные Rate Limiter'ы REST запущены.") // Логируем после инициализации

	// Инициализация Redis клиента
	logger.Log.Infof("Подключение к Redis по адресу %s...", cfg.Redis.Addr)
	redisClient, err := redis.NewRedisClient(cfg.Redis, logger.Log.WithField("component", "redis"))
	if err != nil {
		logger.Log.Fatalf("Не удалось подключиться к Redis по адресу %s: %v", cfg.Redis.Addr, err)
	}
	defer redisClient.Close()
	logger.Log.Info("Подключение к Redis успешно.")

	// Инициализация базы данных
	logger.Log.Infof("Подключение к базе данных '%s' на '%s:%d'...", cfg.Database.DBName, cfg.Database.Host, cfg.Database.Port)
	db, err := database.NewDatabase(cfg.Database, logger.Log.WithField("component", "database"))
	if err != nil {
		logger.Log.Fatalf("Не удалось подключиться к базе данных '%s': %v", cfg.Database.DBName, err)
	}
	defer db.Close()
	logger.Log.Info("Подключение к базе данных успешно.")

	// --- Каналы обмена данными (общие для всех источников) ---
	orderBookMsgChan = make(chan *types.OrderBookMessage, cfg.Channels.OrderBookBufferSize) // Объявлен здесь
	bboChan = make(chan *types.BBO, cfg.Channels.BBOBufferSize)                             // Объявлен здесь

	// --- Каналы команд (специфичные для каждой пары "Биржа:ТипРынка") ---
	// Инициализируем мапы каналов команд. Ключ: "source:market_type".
	// Значение: специфичный канал команд для этого клиента (ИСХОДНЫЙ двунаправленный).
	resyncCmdChannels = make(map[string]chan *types.ResyncCommand)             // *** ИСПРАВЛЕНО: map[string]chan ***
	subscriptionCmdChannels = make(map[string]chan *types.SubscriptionCommand) // *** ИСПРАВЛЕНО: map[string]chan ***

	// 5. Инициализация и запуск биржевых клиентов (по одному экземпляру на каждую включенную пару "Биржа:ТипРынка")

	// Binance Clients
	if cfg.Exchanges.Binance.Enabled {
		logger.Log.WithField("exchange", "binance").Info("Инициализация клиентов Binance...")

		// Binance Spot Client
		if cfg.Exchanges.Binance.Spot.Enabled {
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeSpot}).Infof("Инициализация Binance Spot Client (%s)...", cfg.Exchanges.Binance.Spot.WSBaseURL)
			// Создаем парные каналы для команд ресинка и подписок для Binance Spot
			// Scanner пишет в resyncChanForClient, клиент читает из resyncChanForClient
			resyncChanForClient := make(chan *types.ResyncCommand, cfg.Channels.ResyncCommandBufferSize)
			// SubManager пишет в subChanForClient, клиент читает из subChanForClient
			subChanForClient := make(chan *types.SubscriptionCommand, cfg.Channels.SubscriptionCommandBufferSize)

			// Добавляем ИСХОДНЫЕ двунаправленные каналы в мапы команд для Scanner'а и SubManager'а
			clientKey := fmt.Sprintf("binance:%s", types.MarketTypeSpot)
			resyncCmdChannels[clientKey] = resyncChanForClient    // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***
			subscriptionCmdChannels[clientKey] = subChanForClient // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***

			// Добавляем символы этого рынка в общий список отслеживаемых
			for _, symbol := range cfg.Exchanges.Binance.Spot.GetTrackedSymbols() {
				trackedSymbols[symbol] = struct{}{}
			}

			// Создаем структуру ExchangeChannels для ПЕРЕДАЧИ КЛИЕНТУ
			// Клиент будет ЧИТАТЬ из ResyncChan и SubChan (используем <-chan для типа поля в структуре)
			binanceSpotClientChannels := types.ExchangeChannels{
				OrderBookChan: orderBookMsgChan,    // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				BBOChan:       bboChan,             // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				ResyncChan:    resyncChanForClient, // Специфичный канал команд ресинка (клиент ЧИТАЕТ отсюда <-chan)
				SubChan:       subChanForClient,    // Специфичный канал команд подписки (клиент ЧИТАЕТ отсюда <-chan)
			}
			// Создаем экземпляр клиента Binance Spot
			binanceSpotClient := binance.NewBinanceClient(
				ctx,
				logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeSpot}), // Логгер с контекстом
				cfg.Exchanges.Binance.Spot, // Передаем Spot специфичный конфиг (реализует MarketConfigInterface)
				types.MarketTypeSpot,       // Передаем MarketType
				binanceSpotClientChannels,  // Передаем структуру каналов (<-chan для команд)
				binanceRestLimiter,         // Используем отдельный Binance REST лимитер
			)
			// Запускаем горутину клиента Binance Spot
			wg.Add(1)
			go binanceSpotClient.Run(ctx, &wg)
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeSpot}).Info("Клиент Binance Spot запущен.")
		} else {
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeSpot}).Info("Клиент Binance Spot отключен в конфигурации.")
		}

		// Binance Futures Client (Linear USD-M)
		if cfg.Exchanges.Binance.Futures.Enabled {
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeLinearFutures}).Infof("Инициализация Binance Futures Client (%s)...", cfg.Exchanges.Binance.Futures.WSBaseURL)
			// Создаем специфичные каналы команд для этого клиента Binance Futures
			resyncChanForClient := make(chan *types.ResyncCommand, cfg.Channels.ResyncCommandBufferSize)
			subChanForClient := make(chan *types.SubscriptionCommand, cfg.Channels.SubscriptionCommandBufferSize)

			// Добавляем ИСХОДНЫЕ двунаправленные каналы в мапы команд для Scanner'а и SubManager'а
			clientKey := fmt.Sprintf("binance:%s", types.MarketTypeLinearFutures)
			resyncCmdChannels[clientKey] = resyncChanForClient    // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***
			subscriptionCmdChannels[clientKey] = subChanForClient // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***

			// Добавляем символы этого рынка в общий список отслеживаемых
			for _, symbol := range cfg.Exchanges.Binance.Futures.GetTrackedSymbols() {
				trackedSymbols[symbol] = struct{}{}
			}

			// Создаем структуру ExchangeChannels для ПЕРЕДАЧИ КЛИЕНТУ
			binanceFuturesChannels := types.ExchangeChannels{
				OrderBookChan: orderBookMsgChan,    // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				BBOChan:       bboChan,             // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				ResyncChan:    resyncChanForClient, // Специфичный канал команд ресинка (клиент ЧИТАЕТ отсюда <-chan)
				SubChan:       subChanForClient,    // Специфичный канал команд подписки (клиент ЧИТАЕТ отсюда <-chan)
			}
			// Создаем экземпляр клиента Binance Futures
			binanceFuturesClient := binance.NewBinanceClient(
				ctx,
				logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeLinearFutures}), // Логгер с контекстом
				cfg.Exchanges.Binance.Futures, // Передаем специфичный FuturesConfig (реализует MarketConfigInterface)
				types.MarketTypeLinearFutures, // Передаем MarketType
				binanceFuturesChannels,
				binanceRestLimiter, // Используем отдельный Binance REST лимитер
			)
			// Запускаем горутину клиента Binance Futures
			wg.Add(1)
			go binanceFuturesClient.Run(ctx, &wg)
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeLinearFutures}).Info("Клиент Binance Futures запущен.")
		} else {
			logger.Log.WithFields(logrus.Fields{"exchange": "binance", "market_type": types.MarketTypeLinearFutures}).Info("Клиент Binance Futures отключен в конфигурации.")
		}
	} else {
		logger.Log.Info("Поддержка биржи Binance отключена в конфигурации.")
	}

	// Bybit Clients
	if cfg.Exchanges.Bybit.Enabled {
		logger.Log.WithField("exchange", "bybit").Info("Инициализация клиентов Bybit...")
		// Bybit Spot Client (если включен)
		if cfg.Exchanges.Bybit.Spot.Enabled {
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeSpot}).Infof("Инициализация Bybit Spot Client (%s)...", cfg.Exchanges.Bybit.Spot.WSBaseURL)
			// Создаем специфичные каналы команд для этого клиента Bybit Spot
			resyncChanForClient := make(chan *types.ResyncCommand, cfg.Channels.ResyncCommandBufferSize)
			subChanForClient := make(chan *types.SubscriptionCommand, cfg.Channels.SubscriptionCommandBufferSize)

			// Добавляем ИСХОДНЫЕ двунаправленные каналы в мапы команд для Scanner'а и SubManager'а
			clientKey := fmt.Sprintf("bybit:%s", types.MarketTypeSpot)
			resyncCmdChannels[clientKey] = resyncChanForClient    // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***
			subscriptionCmdChannels[clientKey] = subChanForClient // *** ИСПРАВЛЕНО: Сохраняем исходный chan ***

			// Добавляем символы этого рынка в общий список отслеживаемых
			for _, symbol := range cfg.Exchanges.Bybit.Spot.GetTrackedSymbols() {
				trackedSymbols[symbol] = struct{}{}
			}

			// Создаем структуру ExchangeChannels для ПЕРЕДАЧИ КЛИЕНТУ
			bybitSpotClientChannels := types.ExchangeChannels{ // *** ИСПРАВЛЕНО: Использование переменной bybitSpotClientChannels ***
				OrderBookChan: orderBookMsgChan,    // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				BBOChan:       bboChan,             // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				ResyncChan:    resyncChanForClient, // Специфичный канал команд ресинка (клиент ЧИТАЕТ отсюда <-chan)
				SubChan:       subChanForClient,    // Специфичный канал команд подписки (клиент ЧИТАЕТ отсюда <-chan)
			}
			// Создаем экземпляр клиента Bybit Spot
			bybitSpotClient := bybit.NewBybitClient(
				ctx,
				logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeSpot}), // Логгер с контекстом
				cfg.Exchanges.Bybit.Spot, // Передаем Spot специфичный конфиг
				types.MarketTypeSpot,     // Передаем MarketType
				bybitSpotClientChannels,  // Передаем структуру каналов (<-chan для команд)
				bybitRestLimiter,         // Используем отдельный Bybit REST лимитер
			)
			// Запускаем горутину клиента Bybit Spot
			wg.Add(1)
			go bybitSpotClient.Run(ctx, &wg)
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeSpot}).Info("Bybit Spot Client запущен.")
		} else {
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeSpot}).Info("Bybit Spot Client отключен в конфигурации.")
		}

		// Bybit Futures Client (V5 Linear)
		if cfg.Exchanges.Bybit.Futures.Enabled {
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeLinearFutures}).Infof("Инициализация Bybit Futures Client (%s)...", cfg.Exchanges.Bybit.Futures.WSBaseURL)
			// Создаем специфичные каналы команд для этого клиента Bybit Futures
			resyncChanForClient := make(chan *types.ResyncCommand, cfg.Channels.ResyncCommandBufferSize)
			subChanForClient := make(chan *types.SubscriptionCommand, cfg.Channels.SubscriptionCommandBufferSize)

			// Добавляем ИСХОДНЫЕ двунаправленные каналы в мапы команд для Scanner'а и SubManager'а
			clientKey := fmt.Sprintf("bybit:%s", types.MarketTypeLinearFutures)
			resyncCmdChannels[clientKey] = resyncChanForClient    // *** ИСПРАВЛЕНО: Сохраняем пишущий конец (chan) ***
			subscriptionCmdChannels[clientKey] = subChanForClient // *** ИСПРАВЛЕНО: Сохраняем пишущий конец (chan) ***

			// Добавляем символы этого рынка в общий список отслеживаемых
			for _, symbol := range cfg.Exchanges.Bybit.Futures.GetTrackedSymbols() {
				trackedSymbols[symbol] = struct{}{}
			}

			// Создаем структуру ExchangeChannels для ПЕРЕДАЧИ КЛИЕНТУ
			bybitFuturesChannels := types.ExchangeChannels{
				OrderBookChan: orderBookMsgChan,    // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				BBOChan:       bboChan,             // Общий канал данных (клиент ПИШЕТ сюда chan<-)
				ResyncChan:    resyncChanForClient, // Специфичный канал команд ресинка (клиент ЧИТАЕТ отсюда <-chan)
				SubChan:       subChanForClient,    // Специфичный канал команд подписки (клиент ЧИТАЕТ отсюда <-chan)
			}
			// Создаем экземпляр клиента Bybit Futures
			bybitFuturesClient := bybit.NewBybitClient(
				ctx,
				logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeLinearFutures}), // Логгер с контекстом
				cfg.Exchanges.Bybit.Futures,   // Передаем специфичный FuturesConfig
				types.MarketTypeLinearFutures, // Передаем MarketType
				bybitFuturesChannels,
				bybitRestLimiter, // Используем отдельный Bybit REST лимитер
			)
			// Запускаем горутину клиента Bybit Futures
			wg.Add(1)
			go bybitFuturesClient.Run(ctx, &wg)
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeLinearFutures}).Info("Bybit Futures Client запущен.")
		} else {
			logger.Log.WithFields(logrus.Fields{"exchange": "bybit", "market_type": types.MarketTypeLinearFutures}).Info("Bybit Futures Client отключен в конфигурации.")
		}
	} else {
		logger.Log.Info("Поддержка биржи Bybit отключена в конфигурации.")
	}
	// TODO: Добавить инициализацию других бирж и их рынков аналогичным образом

	// >>> Преобразуем мапу символов в срез для передачи Scanner'у
	symbolsForScanner := make([]string, 0, len(trackedSymbols))
	for symbol := range trackedSymbols {
		symbolsForScanner = append(symbolsForScanner, symbol)
	}
	// Логируем список символов, которые Scanner будет отслеживать
	logger.Log.WithField("symbols", symbolsForScanner).Info("Scanner будет отслеживать следующие символы")

	// 6. Инициализация Scanner
	logger.Log.WithField("component", "scanner").Info("Инициализация Scanner...")
	// Scanner читает из общих каналов данных (<-chan) и пишет в специфичные каналы ресинка (chan<-).
	// Передаем Scanner'у мапу ВСЕХ каналов ресинка И список символов для отслеживания.

	// *** ИСПРАВЛЕНИЕ ТИПА КАНАЛОВ ДЛЯ SCANNER ***
	// Создаем временную мапу с типом map[string]chan<-
	scannerResyncChannels := make(map[string]chan<- *types.ResyncCommand, len(resyncCmdChannels))
	// Копируем каналы из исходной мапы, преобразуя тип каждого канала в chan<-
	for key, ch := range resyncCmdChannels {
		// Проверяем на nil на всякий случай, хотя при make и последующем присваивании не должно быть nil
		if ch != nil {
			scannerResyncChannels[key] = ch // Присваивание chan к chan<- разрешено
		}
	}
	// Теперь передаем временную мапу в конструктор Scanner'а
	scannerInstance := scanner.NewScanner(
		ctx,
		logger.Log.WithField("component", "scanner"), // 1 (logrus.Entry)
		orderBookMsgChan,      // 2 (<-chan)
		bboChan,               // 3 (<-chan)
		redisClient,           // 4
		scannerResyncChannels, // 5 (map[string]chan<- *types.ResyncCommand) *** ТИП ТЕПЕРЬ СОВПАДАЕТ ***
		symbolsForScanner,     // 6 ([]string)
		&wg,                   // 8 (*sync.WaitGroup) - wg для Scanner Run goroutine
	)
	// Запускаем горутину Scanner
	wg.Add(1) // Добавляем горутину Scanner в WaitGroup
	// Метод Run Scanner'а должен принимать контекст и WaitGroup, если еще не так
	go scannerInstance.Run(ctx, &wg) // Scanner Run defer добавит и Done wg
	logger.Log.WithField("component", "scanner").Info("Scanner запущен.")

	// 7. Инициализация Subscription Manager
	if cfg.SubscriptionManager.Enabled {
		logger.Log.WithField("component", "sub_manager").Info("Инициализация Subscription Manager...")
		// SubManager пишет в специфичные каналы подписки (chan<-).
		// Передаем SubManager'у мапу ВСЕХ каналов подписки.
		subscriptionManager := subscription.NewSubscriptionManager(
			ctx,
			logger.Log.WithField("component", "sub_manager"), // Логгер с контекстом для SubManager'а (logrus.Entry)
			cfg.SubscriptionManager,
			// Передаем мапу исходных chan, Go автоматически преобразует в map[string]chan<- для параметра NewSubscriptionManager
			subscriptionCmdChannels, // *** ПЕРЕДАЕМ МАПУ КАНАЛОВ ПОДПИСКИ (map[string]chan) -> (map[string]chan<-) ***
		)
		// Запускаем горутину Subscription Manager
		wg.Add(1)                            // Добавляем горутину SubManager в WaitGroup
		go subscriptionManager.Run(ctx, &wg) // Метод Run должен принимать контекст и WaitGroup
		logger.Log.WithField("component", "sub_manager").Info("Subscription Manager запущен.")

		// TODO: Передать subscriptionManager в Arbitrage Analyzer, если он будет инициировать подписки динамически.
		// analyzer.SetSubscriptionManager(subscriptionManager) // Потребуется новый метод в Analyzer
	} else {
		logger.Log.WithField("component", "sub_manager").Info("Subscription Manager отключен в конфигурации.")
	}

	// 8. Инициализация Arbitrage Analyzer
	if cfg.ArbitrageAnalyzer.Enabled {
		logger.Log.WithField("component", "arbitrage_analyzer").Info("Инициализация Arbitrage Analyzer...")
		// Analyzer читает из Scanner'а.
		// TODO: Передать SubscriptionManager в Analyzer, если нужно динамическое управление подписками.
		analyzer := arbitrage.NewArbitrageAnalyzer(
			ctx,
			logger.Log.WithField("component", "arbitrage_analyzer"), // Логгер с контекстом для Analyzer'а (logrus.Entry)
			scannerInstance,       // Scanner (теперь работает с типами маркетов)
			db,                    // База данных
			cfg.ArbitrageAnalyzer, // Конфигурация анализатора (теперь содержит "биржа:тип_рынка")
			// TODO: Добавить subscriptionManager, если нужно
		)
		// Запускаем горутину Analyzer
		wg.Add(1)                 // Добавляем горутину Analyzer в WaitGroup
		go analyzer.Run(ctx, &wg) // Метод Run должен принимать контекст и WaitGroup
		logger.Log.WithField("component", "arbitrage_analyzer").Info("Arbitrage Analyzer запущен.")
	} else {
		logger.Log.WithField("component", "arbitrage_analyzer").Info("Arbitrage Analyzer отключен в конфигурации.")
	}

	if cfg.Web.Enabled {
		logger.Log.WithField("component", "web_server").Info("Инициализация веб-сервера...") // Используем logrus logger Info
		// Web Server читает из Scanner'а.
		webServer := web.NewWebSocketServer(
			ctx,
			&wg,             // WaitGroup из main.go
			scannerInstance, // Scanner
			logger.Log.WithField("component", "web_server"), // <<< ПЕРЕДАЕМ ЛОГГЕР (logrus.Entry)
		)
		// Запускаем горутину веб-сервера
		// wg.Add(1) // Add is inside StartServer now
		go web.StartServer(
			ctx,
			cfg.Web.ListenAddr,
			webServer,
			&wg, // WaitGroup из main.go
			logger.Log.WithField("component", "web_server").WithField("server_addr", cfg.Web.ListenAddr), // <<< ПЕРЕДАЕМ ЛОГГЕР (logrus.Entry)
		)
		// Note: StartServer itself adds to wg and defers wg.Done()
		logger.Log.WithField("component", "web_server").Infof("Веб-сервер запущен на адресе %s.", cfg.Web.ListenAddr) // Используем logrus logger Infof
	} else {
		logger.Log.WithField("component", "web_server").Info("Веб-сервер отключен в конфигурации.") // Используем logrus logger Info
	}

	// --- ИНИЦИАЛИЗАЦИЯ НАЧАЛЬНЫХ ПОДПИСОК (В SubscriptionManager) ---
	// --- ИНИЦИАЛИЗАЦИЯ НАЧАЛЬНЫХ РЕСИНКОВ (Инициируется Scanner'ом после получения данных или разрыва) ---

	// 9. Обработка сигнала завершения (Ctrl+C и т.д.)
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	logger.Log.Info("Приложение запущено. Ожидание сигнала завершения (Ctrl+C) или отмены контекста...")
	// Этот select ожидает либо сигнала ОС, либо отмены контекста.
	select {
	case <-stopSignal:
		logger.Log.Info("Получен сигнал завершения ОС. Инициирование плавного останова...")
	case <-ctx.Done():
		// Если контекст отменяется раньше (например, из-за фатальной ошибки в какой-то горутине),
		// мы также инициируем завершение.
		logger.Log.Info("Контекст приложения отменен. Инициирование плавного останова...")
	}

	// 10. Запуск процесса корректного завершения
	// Отменяем главный контекст. Это сигнализирует всем компонентам о завершении (через ctx.Done()).
	cancel() // Вызываем cancel() здесь, чтобы все горутины получили сигнал.

	// Ожидаем завершения всех горутин, добавленных в WaitGroup.
	logger.Log.Info("Ожидание завершения всех компонентов...")
	wg.Wait() // Блокируется до тех пор, пока счетчик WaitGroup не станет равным нулю.

	// 11. Закрываем общие каналы данных.
	// Эти каналы закрываются ПОСЛЕ того, как wg.Wait() завершился,
	// что означает, что все горутины (включая клиенты, которые писали в эти каналы,
	// и Scanner, который из них читал) уже завершились. Это безопасно.
	// Логируем закрытие каналов
	logger.Log.Debug("main: Закрываю общие каналы данных OrderBookChan и BBOChan...") // Уровень Debug, т.к. это детализация процесса завершения
	// Перед закрытием каналов, убедимся, что они не равны nil (если какой-то клиент не был инициализирован).
	if orderBookMsgChan != nil {
		close(orderBookMsgChan)
	}
	if bboChan != nil {
		close(bboChan)
	}
	logger.Log.Debug("main: Общие каналы данных закрыты.")

	// 12. Закрываем специфичные каналы команд.
	// Логируем закрытие каналов команд
	logger.Log.Debug("main: Закрываю специфичные каналы команд ресинка и подписки...") // Уровень Debug
	// Итерируем по мапам, которые хранят исходные chan.
	// Проверим на nil перед итерацией (если какая-то мапа не была инициализирована, т.е. ни один клиент не был включен)
	if resyncCmdChannels != nil {
		for key, resyncChan := range resyncCmdChannels {
			// Проверим на nil перед закрытием (если мапа была инициализирована, но канал не был добавлен для какого-то ключа - не должно происходить при текущей логике)
			if resyncChan != nil {
				close(resyncChan) // Закрываем исходный двунаправленный канал
				logger.Log.Debugf("main: Канал ресинка %s закрыт.", key)
			}
		}
	}
	if subscriptionCmdChannels != nil {
		for key, subChan := range subscriptionCmdChannels {
			if subChan != nil {
				close(subChan) // Закрываем исходный двунаправленный канал
				logger.Log.Debugf("main: Канал подписки %s закрыт.", key)
			}
		}
	}
	logger.Log.Debug("main: Специфичные каналы команд закрыты.")

	// Defer-вызовы (redisClient.Close, db.Close, RateLimiter.Stop)
	// выполнятся автоматически после выхода из main().

	logger.Log.Info("Все компоненты остановлены. Приложение завершено.")
}

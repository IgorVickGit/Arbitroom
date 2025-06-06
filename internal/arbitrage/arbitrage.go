package arbitrage

import (
	"context"
	// "log" // >>> Удаляем стандартный лог
	"sync"
	"time"

	// Убедитесь, что путь к вашему конфигу правильный
	"arbitrage-engine/internal/config"
	// Зависимость от пакета БД
	"arbitrage-engine/internal/database"
	// Зависимость от пакета Scanner
	"arbitrage-engine/internal/scanner"

	"github.com/sirupsen/logrus"
)

// ArbitrageAnalyzer ищет и логирует арбитражные возможности
type ArbitrageAnalyzer struct {
	ctx     context.Context
	log     *logrus.Entry      // >>> Используем *logrus.Entry
	scanner *scanner.Scanner   // Для чтения актуальных BBO из Redis
	db      *database.Database // Для доступа к информации о рынках/сетях (будет использоваться позже)
	cfg     config.ArbitrageAnalyzerConfig
}

// NewArbitrageAnalyzer создает новый экземпляр ArbitrageAnalyzer
// Принимает контекст, логгер (logrus.Entry), Scanner, БД и конфигурацию анализатора
// *** ОБНОВЛЕННАЯ СИГНАТУРА ***
func NewArbitrageAnalyzer(
	ctx context.Context,
	logger *logrus.Entry, // >>> Принимаем logrus logger
	scanner *scanner.Scanner,
	db *database.Database,
	cfg config.ArbitrageAnalyzerConfig,
) *ArbitrageAnalyzer {
	// Создаем логгер с контекстом для данного компонента
	logger = logger.WithField("component", "arbitrage_analyzer")
	logger.Info("Начало инициализации ArbitrageAnalyzer...") // Используем logrus logger

	analyzer := &ArbitrageAnalyzer{
		ctx:     ctx,
		log:     logger, // >>> Сохраняем logrus logger
		scanner: scanner,
		db:      db,
		cfg:     cfg,
	}

	logger.Info("Инициализация ArbitrageAnalyzer завершена.") // Используем logrus logger

	return analyzer
}

// Run запускает цикл поиска арбитражных возможностей
func (a *ArbitrageAnalyzer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done() // Уведомляем WaitGroup о завершении при выходе

	a.log.Info("Arbitrage Analyzer запущен.")          // Используем logrus logger
	defer a.log.Info("Arbitrage Analyzer остановлен.") // Используем logrus logger

	interval := a.cfg.CheckInterval
	if interval <= 0 {
		// Логируем предупреждение о некорректном интервале
		a.log.WithField("config_interval", a.cfg.CheckInterval).Warnf("Некорректный интервал проверки. Использую интервал по умолчанию %s.", time.Second.String()) // Используем logrus logger Warnf
		interval = time.Second
	} else {
		a.log.WithField("check_interval", interval).Info("Интервал проверки арбитражных возможностей установлен.") // Логируем установленный интервал
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop() // Остановка тикера при выходе из функции

	a.log.Info("Arbitrage Analyzer: Ожидание тикера или сигнала отмены контекста...")

	for {
		select {
		case <-ticker.C:
			// Время выполнить проверку
			a.checkOpportunities() // checkOpportunities использует logrus внутри

		case <-a.ctx.Done():
			// Получен сигнал на завершение
			a.log.Info("Arbitrage Analyzer: Получен сигнал отмены контекста. Завершение работы...") // Используем logrus logger Info
			return
		}
	}
}

// checkOpportunities выполняет один цикл поиска арбитражных возможностей
// Реальная логика сравнения BBO. Использует logrus.
func (a *ArbitrageAnalyzer) checkOpportunities() {
	// Используем логгер с контекстом для этого цикла проверки
	logger := a.log.WithField("func", "checkOpportunities")
	logger.Debugf("Проверка арбитражных возможностей для символов %v на рынках %v...", a.cfg.Symbols, a.cfg.Exchanges) // Используем logrus logger Debugf

	// Check if symbols and exchanges are configured
	if len(a.cfg.Symbols) == 0 || len(a.cfg.Exchanges) < 2 {
		logger.Debug("Недостаточно символов или рынков в конфигурации для анализа. Пропуск проверки.")
		return // Нет смысла продолжать
	}

	// 1. Пройтись по всем символам для анализа из конфига
	for _, symbol := range a.cfg.Symbols {
		// Добавляем символ в контекст логгера для этого символа
		symbolLogger := logger.WithField("symbol", symbol)
		symbolLogger.Trace("Проверка для символа...") // Trace

		// 2. Для каждого символа, получить BBO со всех рынков для анализа из конфига
		// a.cfg.Exchanges is now a slice of strings like ["binance:spot", "bybit:linear_futures"]
		// Scanner.GetBBOsForSymbol is updated to accept this format and return map[string]types.BBO
		bbos, err := a.scanner.GetBBOsForSymbol(a.ctx, symbol, a.cfg.Exchanges) // Scanner is now compatible, uses logrus internally
		if err != nil {
			symbolLogger.WithError(err).Errorf("Ошибка при получении BBO с рынков %v.", a.cfg.Exchanges) // Используем logrus logger Errorf
			continue                                                                                     // Переходим к следующему символу при ошибке
		}

		// Проверяем, что мы получили данные хотя бы с двух рынков
		if len(bbos) < 2 {
			symbolLogger.Debugf("Недостаточно данных BBO (%d) для анализа с рынков %v.", len(bbos), a.cfg.Exchanges) // Используем logrus logger Debugf
			continue                                                                                                 // Нет смысла искать арбитраж, если данных меньше чем с 2х рынков
		}

		symbolLogger.Tracef("Получены BBO с %d рынков.", len(bbos)) // Trace

		// 3. Найти Max(Bid) и Min(Ask) среди полученных BBO с учетом биржи И ТИПА РЫНКА
		var maxBid float64 = 0
		var maxBidSourceMarket string // Store the key "source:market_type"
		var minAsk float64 = 0        // Initialize with a large value
		var minAskSourceMarket string // Store the key "source:market_type"
		firstAsk := true              // Flag for initializing minAsk with the first valid ask value

		// Iterate through the map returned by Scanner. Key is "source:market_type".
		for sourceMarketKey, bbo := range bbos {
			// Check if BBO data is valid (prices > 0) before considering.
			// A price of 0.0 could indicate lack of liquidity or a problem.
			if bbo.BestBid > 0 {
				// Find maximum Bid
				if bbo.BestBid > maxBid {
					maxBid = bbo.BestBid
					maxBidSourceMarket = sourceMarketKey // Save the source:market_type key
				}
				// Log the BBO data on Trace level for debugging
				symbolLogger.WithFields(logrus.Fields{
					"market": sourceMarketKey, "bid": bbo.BestBid, "bid_qty": bbo.BestBidQuantity,
				}).Trace("Обработка Bid BBO.")
			} else {
				// Log if BBO has invalid bid price (optional, can be noisy)
				symbolLogger.WithFields(logrus.Fields{
					"market": sourceMarketKey, "bid": bbo.BestBid,
				}).Trace("Игнорирование невалидного Bid BBO.") // Trace
			}

			if bbo.BestAsk > 0 {
				// Find minimum Ask
				if firstAsk || bbo.BestAsk < minAsk {
					minAsk = bbo.BestAsk
					minAskSourceMarket = sourceMarketKey // Save the source:market_type key
					firstAsk = false                     // First valid ask value processed
				}
				// Log the BBO data on Trace level for debugging
				symbolLogger.WithFields(logrus.Fields{
					"market": sourceMarketKey, "ask": bbo.BestAsk, "ask_qty": bbo.BestAskQuantity,
				}).Trace("Обработка Ask BBO.")

			} else {
				// Log if BBO has invalid ask price (optional, can be noisy)
				symbolLogger.WithFields(logrus.Fields{
					"market": sourceMarketKey, "ask": bbo.BestAsk,
				}).Trace("Игнорирование невалидного Ask BBO.") // Trace
			}
		}

		// Проверяем, что нашли MaxBid и MinAsk, они > 0, и они с РАЗНЫХ РЫНКОВ (разных комбинаций биржа:тип_рынка)
		if maxBid > 0 && minAsk > 0 && maxBidSourceMarket != "" && minAskSourceMarket != "" && maxBidSourceMarket != minAskSourceMarket {
			// 4. Если Max(Bid) > Min(Ask), залогировать возможность.
			if maxBid > minAsk {
				// Это потенциальная арбитражная возможность!
				// TODO: Рассчитать спред с учетом комиссий
				// TODO: Учесть минимальные размеры ордеров и доступный баланс
				// TODO: Рассчитать потенциальную прибыль
				spreadPercent := ((maxBid / minAsk) - 1) * 100

				// Логируем найденную возможность с указанием биржи и типа рынка для покупки и продажи.
				// Используем WithFields для структурированного лога
				symbolLogger.WithFields(logrus.Fields{
					"max_bid":        maxBid,
					"max_bid_market": maxBidSourceMarket,
					"min_ask":        minAsk,
					"min_ask_market": minAskSourceMarket,
					"spread_percent": spreadPercent,
				}).Info("!!! Потенциальный арбитраж найден !!!") // Используем logrus logger Info

				// TODO: На этом этапе:
				// 1. Проверить, подписаны ли мы уже на глубину стакана для minAskSourceMarket и maxBidSourceMarket.
				//    Можно хранить список активных подписок на глубину в ArbitrageAnalyzer или запросить статус у Scanner'а (Scanner.GetSyncMetadata).
				//    Scanner.GetSyncMetadata(source, marketType, symbol) (*OrderBookSyncMetadata, bool) - этот метод уже есть и принимает marketType.
				//    Проверка статуса подписки на глубину:
				//    buyMarketMetadata, buyMarketFound := a.scanner.GetSyncMetadata(buySource, buyMarketType, symbol)
				//    sellMarketMetadata, sellMarketFound := a.scanner.GetSyncMetadata(sellSource, sellMarketType, symbol)
				//    Если buyMarketFound && sellMarketFound && buyMarketMetadata.CurrentStatus == "Synced" && sellMarketMetadata.CurrentStatus == "Synced" && buyMarketMetadata.BybitDepth >= requiredDepth && sellMarketMetadata.BybitDepth >= requiredDepth { ... уже синхронизированы с достаточной глубиной ... }

				// 2. Если не подписаны (или подписаны на недостаточную глубину), сформировать команду SubscriptionCommand (SubscribeDepth) для minAskSourceMarket и maxBidSourceMarket.
				//    Разбиваем "source:market_type" на source и marketType.
				//    buyParts := strings.Split(minAskSourceMarket, ":") // Купить на MinAsk
				//    sellParts := strings.Split(maxBidSourceMarket, ":") // Продать на MaxBid
				//    if len(buyParts) == 2 && len(sellParts) == 2 {
				//        buySource, buyMarketTypeStr := buyParts[0], buyParts[1]
				//        sellSource, sellMarketTypeStr := sellParts[0], sellParts[1]
				//        buyMarketType := types.MarketType(buyMarketTypeStr)
				//        sellMarketType := types.MarketType(sellMarketTypeStr)
				//        requiredDepth := 50 // TODO: Глубина из конфига анализатора? Или динамически?

				//        // Проверяем статус подписки на глубину для рынка покупки
				//        buyMarketMetadata, buyMarketFound := a.scanner.GetSyncMetadata(buySource, buyMarketType, symbol)
				//        if !buyMarketFound || buyMarketMetadata.CurrentStatus != "Synced" || buyMarketMetadata.BybitDepth < requiredDepth {
				//            // Нужна подписка на глубину для рынка покупки
				//            cmd := types.SubscriptionCommand{Source: buySource, MarketType: buyMarketType, Symbol: symbol, Type: types.SubscribeDepth, Depth: requiredDepth}
				//            // TODO: Отправить команду в SubscriptionManager. analyzer должен иметь ссылку на SubscriptionManager
				//            // err := a.subscriptionManager.SendCommand(cmd)
				//            // if err != nil { logger.WithError(err).WithField("command", cmd).Error("Ошибка отправки команды подписки на глубину."); } else { logger.WithField("command", cmd).Info("Отправлена команда подписки на глубину."); }
				//        }

				//        // Проверяем статус подписки на глубину для рынка продажи
				//        sellMarketMetadata, sellMarketFound := a.scanner.GetSyncMetadata(sellSource, sellMarketType, symbol)
				//        if !sellMarketFound || sellMarketMetadata.CurrentStatus != "Synced" || sellMarketMetadata.BybitDepth < requiredDepth {
				//            // Нужна подписка на глубину для рынка продажи
				//            cmd := types.SubscriptionCommand{Source: sellSource, MarketType: sellMarketType, Symbol: symbol, Type: types.SubscribeDepth, Depth: requiredDepth}
				//            // TODO: Отправить команду в SubscriptionManager
				//            // err := a.subscriptionManager.SendCommand(cmd)
				//            // if err != nil { logger.WithError(err).WithField("command", cmd).Error("Ошибка отправки команды подписки на глубину."); } else { logger.WithField("command", cmd).Info("Отправлена команда подписки на глубину."); }
				//        }
				//    } else {
				//        logger.WithFields(logrus.Fields{"buy_market": minAskSourceMarket, "sell_market": maxBidSourceMarket}).Error("Некорректный формат source:market_type для арбитражной возможности.")
				//    }

				// TODO: Если уже подписаны на достаточную глубину, запросить актуальные уровни стакана из Scanner'а (GetOrderBookLevels).
				// Scanner.GetOrderBookLevels(source, marketType, symbol, limit) ([]types.PriceLevel, []types.PriceLevel, int64, time.Time, error)
				// buyBids, buyAsks, _, _, err := a.scanner.GetOrderBookLevels(buySource, buyMarketType, symbol, requiredDepth)
				// sellBids, sellAsks, _, _, err := a.scanner.GetOrderBookLevels(sellSource, sellMarketType, symbol, requiredDepth)
				// TODO: Выполнить более детальный расчет потенциала с учетом комиссии и ликвидности по уровням.
				// TODO: Если потенциал подтвержден, отправить сигнал в Order Executor (в будущих шагах).
				// a.orderExecutor.ExecuteArbitrage(...)
			}
		}
		// else {
		//     // Debug log if no opportunity found (optional, can be noisy)
		//     if maxBid > 0 && minAsk > 0 { // Only log if we actually found valid bids/asks but no spread
		//          symbolLogger.Tracef("Арбитраж для символа %s не найден в этом цикле (MaxBid=%.8f на %s, MinAsk=%.8f на %s)", symbol, maxBid, maxBidSourceMarket, minAsk, minAskSourceMarket);
		//     }
		// }
	}
}

// NOTE: В будущем здесь могут появиться методы для использования БД,
// например, для получения списка активных маркетов или комиссий.
// Эти методы также должны принимать context.Context и использовать a.log для логирования.
// func (a *ArbitrageAnalyzer) getMarketInfoFromDB(ctx context.Context, ...) {
//     logger := a.log.WithField("func", "getMarketInfoFromDB"); // Добавляем контекст
//     logger.Debug("Получение информации о маркете из БД.");
//     // ... выполнение запроса к БД с использованием ctx ...
//     // Логируем ошибки запроса или парсинга
// }

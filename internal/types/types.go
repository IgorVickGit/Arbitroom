package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// MarketType определяет тип рынка (spot, linear_futures, coin_futures и т.д.)
type MarketType string

const (
	MarketTypeUnknown       MarketType = "unknown"
	MarketTypeSpot          MarketType = "spot"
	MarketTypeLinearFutures MarketType = "linear_futures" // Бессрочные фьючерсы USD-M
	MarketTypeCoinFutures   MarketType = "coin_futures"   // Бессрочные фьючерсы Coin-M
	// Добавьте другие типы маркетов при необходимости (например, options, inverse_futures)
)

// PriceLevel представляет собой уровень цены в стакане (цена + количество)
type PriceLevel struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

// OrderBook представляет собой snapshot стакана (Эта структура может использоваться напрямую для ответов REST)
type OrderBook struct {
	Symbol       string       `json:"symbol"`
	Bids         []PriceLevel `json:"bids"`
	Asks         []PriceLevel `json:"asks"`
	Timestamp    time.Time    `json:"timestamp"` // Время получения данных
	Source       string       `json:"source"`
	MarketType   MarketType   `json:"market_type"`  // *** ДОБАВЛЕНО *** Тип рынка (spot, linear_futures и т.д.)
	LastUpdateID int64        `json:"lastUpdateId"` // ID последнего обновления в snapshot (Binance REST)
}

// BBO (Best Bid and Offer) представляет лучшие цены на покупку и продажу
type BBO struct {
	Symbol          string     `json:"symbol"`
	BestBid         float64    `json:"best_bid"`
	BestAsk         float64    `json:"best_ask"`
	Timestamp       time.Time  `json:"timestamp"`
	Source          string     `json:"source"`
	MarketType      MarketType `json:"market_type"` // *** ДОБАВЛЕНО *** Тип рынка
	BestBidQuantity float64    `json:"best_bid_quantity"`
	BestAskQuantity float64    `json:"best_ask_quantity"`
}

// MarshalBinary implements encoding.BinaryMarshaler for Redis storage
func (b *BBO) MarshalBinary() ([]byte, error) {
	return json.Marshal(b)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler for Redis retrieval
func (b *BBO) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, b)
}

// OrderBookDelta представляет собой частичное обновление стакана
type OrderBookDelta struct {
	Symbol     string       `json:"symbol"`
	Bids       []PriceLevel `json:"bids"` // Изменения на покупку (пустые или с новыми/измененными/удаленными уровнями)
	Asks       []PriceLevel `json:"asks"` // Изменения на продажу
	Timestamp  time.Time    `json:"timestamp"`
	Source     string       `json:"source"`      // Redundant if in main message, but useful for internal consistency
	MarketType MarketType   `json:"market_type"` // *** ДОБАВЛЕНО *** Тип рынка
	// Поля для управления версиями/снепшотами, если биржа их предоставляет
	FirstUpdateID int64 `json:"first_update_id,omitempty"` // Идентификатор первого обновления в этом сообщении (U у Binance)
	FinalUpdateID int64 `json:"final_update_id"`           // Идентификатор последнего обновления в этом сообщении (u у Binance/Bybit)
}

// OrderBookMessage представляет унифицированное сообщение стакана (снимок или дельта).
type OrderBookMessage struct {
	Source     string          `json:"source"`      // Биржа-источник ("binance", "bybit")
	MarketType MarketType      `json:"market_type"` // *** ДОБАВЛЕНО *** Тип рынка
	Symbol     string          `json:"symbol"`      // Торговая пара (в верхнем регистре)
	Type       string          `json:"type"`        // Тип сообщения (например, "snapshot_rest", "snapshot", "delta", "bookTicker")
	Timestamp  time.Time       `json:"timestamp"`   // Время события (время получения или время из сообщения биржи)
	Delta      *OrderBookDelta `json:"delta"`       // Данные дельты/snapshot'а. Для snapshot_rest и snapshot содержит полный набор уровней. Для delta - только изменения.
	RawData    json.RawMessage `json:"raw_data"`    // Сырые данные сообщения от биржи (для парсинга специфичных полей типа топика)
	// Можно добавить другие поля, если понадобятся (например, Topic string для Bybit V5)
}

// ResyncCommandType определяет тип команды ресинхронизации
type ResyncCommandType string

const (
	ResyncTypeBinanceSnapshot  ResyncCommandType = "binance_snapshot"  // Запросить REST snapshot для Binance (нужен MarketType)
	ResyncTypeBybitResubscribe ResyncCommandType = "bybit_resubscribe" // Переподписаться на WS для Bybit (нужен MarketType)
	// Добавьте другие типы команд ресинка при необходимости
)

// ResyncCommand структура команды для запуска ресинхронизации клиентом
type ResyncCommand struct {
	Source     string            `json:"source"`      // Биржа, для которой нужна команда (например, "binance", "bybit")
	MarketType MarketType        `json:"market_type"` // Тип рынка (spot, linear_futures и т.д.)
	Symbol     string            `json:"symbol"`      // Символ, для которого нужен ресинк (в верхнем регистре)
	Type       ResyncCommandType `json:"type"`        // Тип команды ресинка
	// Дополнительные параметры могут быть добавлены в зависимости от типа команды
	Depth string `json:"depth,omitempty"` // Используется для BybitResubscribe (глубина для переподписки)
}

// --- Новые типы для управления подписками ---

// SubscriptionCommandType определяет тип команды управления подпиской
type SubscriptionCommandType string

const (
	SubscribeDepth   SubscriptionCommandType = "subscribe_depth"   // Подписаться на стакан с определенной глубиной
	UnsubscribeDepth SubscriptionCommandType = "unsubscribe_depth" // Отписаться от стакана с определенной глубиной
	SubscribeBBO     SubscriptionCommandType = "subscribe_bbo"     // Подписаться на BBO стрим
	UnsubscribeBBO   SubscriptionCommandType = "unsubscribe_bbo"   // Отписаться от BBO стрим
	// Добавьте другие типы команд подписки (например, SubscribeTrades, UnsubscribeTrades)
)

// SubscriptionCommand структура команды для клиента биржи для управления подписками
// Отправляется SubscriptionManager'ом клиенту.
// SubscriptionCommand структура команды для клиента биржи для управления подписками
type SubscriptionCommand struct {
	Source     string                  `json:"source" yaml:"source"`           // Биржа, для которой предназначена команда
	MarketType MarketType              `json:"market_type" yaml:"market_type"` // *** ДОБАВЛЕНО/ИСПРАВЛЕНО: ЯВНЫЙ YAML ТЕГ ***
	Symbol     string                  `json:"symbol" yaml:"symbol"`           // Символ (в верхнем регистре)
	Type       SubscriptionCommandType `json:"type" yaml:"type"`               // Тип команды подписки/отписки

	// Параметры команды (зависят от Type)
	Depth int `json:"depth,omitempty" yaml:"depth,omitempty"` // Используется для SubscribeDepth/UnsubscribeDepth (например, 50, 1000)
}

func (b *BBO) String() string {
	// Corrected line 135 (assuming this is the line number in your file)
	return fmt.Sprintf("Source=%s, Market=%s, Symbol=%s, Bid=%.8f (Qty=%.8f), Ask=%.8f (Qty=%.8f), Timestamp=%v",
		b.Source, b.MarketType, b.Symbol, b.BestBid, b.BestBidQuantity, b.BestAsk, b.BestAskQuantity, b.Timestamp)
}

// --- Каналы для обмена данными между компонентами ---

// ExchangeChannels объединяет все каналы для обмена данными с одной биржей/типом рынка.
// Эта структура передается КЛИЕНТУ биржи в конструкторе.
// Каналы OrderBookChan и BBOChan - КЛИЕНТ ПИШЕТ (chan<-).
// Каналы ResyncChan и SubChan - КЛИЕНТ ЧИТАЕТ (<-chan).
type ExchangeChannels struct {
	OrderBookChan chan<- *OrderBookMessage    // Канал для отправки OrderBookMessage в Scanner (клиент пишет)
	BBOChan       chan<- *BBO                 // Канал для отправки BBO в Scanner (клиент пишет)
	ResyncChan    <-chan *ResyncCommand       // *** ИСПРАВЛЕНО: Клиент ЧИТАЕТ команды ресинка от Scanner'а ***
	SubChan       <-chan *SubscriptionCommand // *** ИСПРАВЛЕНО: Клиент ЧИТАЕТ команды подписки/отписки от SubscriptionManager'а ***
}

// --- Типы для данных, отправляемых на веб-страницу по WS ---

// WebDataMessage структура, отправляемая по WebSocket на веб-страницу
// Убрана вложенность по MarketType здесь для простоты, WebExchangeData теперь содержит MarketType.
// В WebExchangeData.Data[source] мы теперь будем хранить мапу: Symbol -> MarketType -> WebExchangeData
// Или, возможно, Symbol -> map[MarketType]WebExchangeData
// Давайте сделаем Source -> Symbol -> MarketType -> WebExchangeData для максимальной гибкости на фронтенде
type WebDataMessage struct {
	Type string                                               `json:"type"` // Например, "orderbook_update", "bbo_update", "error"
	Data map[string]map[string]map[MarketType]WebExchangeData `json:"data"` // Карта: Source -> Symbol -> MarketType -> Данные по бирже/символу/типу рынка
}

// WebExchangeData объединяет данные для конкретной биржи/символа/типа рынка на веб-странице
type WebExchangeData struct {
	BBO            *BBO          `json:"bbo,omitempty"`              // Текущий BBO для этой пары/типа рынка
	OrderBook      *OrderBookWeb `json:"orderbook,omitempty"`        // Топ N уровней стакана для этой пары/типа рынка
	SyncStatus     string        `json:"sync_status,omitempty"`      // Например, "Initializing", "Synced", "Syncing", "Gap Detected, Resyncing", "Disconnected"
	LastUpdateID   int64         `json:"last_update_id,omitempty"`   // Последний UpdateID стакана
	LastUpdateTime time.Time     `json:"last_update_time,omitempty"` // Время последнего обновления стакана
	MarketType     MarketType    `json:"market_type"`                // *** ДОБАВЛЕНО *** Тип рынка
	Source         string        `json:"source"`                     // *** ДОБАВЛЕНО *** Биржа
	Symbol         string        `json:"symbol"`                     // *** ДОБАВЛЕНО *** Символ
}

// OrderBookWeb структура для отправки ТОП N уровней стакана на веб-страницу
type OrderBookWeb struct {
	Symbol string       `json:"symbol"`
	Bids   []PriceLevel `json:"bids"` // Топ N покупок
	Asks   []PriceLevel `json:"asks"` // Топ N продаж
}

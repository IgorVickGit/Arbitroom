// internal/config/config.go
package config

import (
	"arbitrage-engine/internal/types" // Import пакета types для MarketType и SubscriptionCommand
	"fmt"
	"io/ioutil"
	"os" // Импорт для os.Stderr

	// "log" // Можно удалить, если переходим на fmt.Fprintf для ошибок
	"time"

	"gopkg.in/yaml.v2" // Импорт для парсинга YAML
)

// MarketConfigInterface определяет общие методы для конфигурации рынков (Spot, Futures)
type MarketConfigInterface interface {
	IsEnabled() bool
	GetWSBaseURL() string
	GetRESTBaseURL() string
	GetWSMaxTopicsPerSubscribe() int
	GetWSSendMessageRateLimitPerSec() int
	GetTrackedSymbols() []string // Добавим этот метод для доступа к символам из интерфейса
}

// Реализация GetTrackedSymbols для SpotConfig
func (c SpotConfig) GetTrackedSymbols() []string { return c.TrackedSymbols }

// Реализация GetTrackedSymbols для FuturesConfig
func (c FuturesConfig) GetTrackedSymbols() []string { return c.TrackedSymbols }

// Config - главная структура для всей конфигурации приложения
type Config struct {
	Log                 LogConfig                 `yaml:"log"` // ДОБАВЛЕНА: Конфигурация логирования
	Redis               RedisConfig               `yaml:"redis"`
	Database            DatabaseConfig            `yaml:"database"`
	Channels            ChannelsConfig            `yaml:"channels"`
	Exchanges           ExchangesConfig           `yaml:"exchanges"`
	Scanner             ScannerConfig             `yaml:"scanner"`
	ArbitrageAnalyzer   ArbitrageAnalyzerConfig   `yaml:"arbitrage_analyzer"`
	Web                 WebConfig                 `yaml:"web"`
	RateLimits          RateLimitsConfig          `yaml:"rate_limits,omitempty"`
	SubscriptionManager SubscriptionManagerConfig `yaml:"subscription_manager"`
	// Добавьте другие глобальные настройки здесь
}

// ДОБАВЛЕНА: LogConfig - конфигурация для логирования
type LogConfig struct {
	Level      string `yaml:"level" env:"LOG_LEVEL"`             // Уровень логирования (trace, debug, info, warn, error, fatal, panic)
	FilePath   string `yaml:"file_path" env:"LOG_FILE_PATH"`     // Путь к файлу логов (если пусто - только консоль)
	MaxSize    int    `yaml:"max_size" env:"LOG_MAX_SIZE"`       // Макс. размер файла лога в МБ перед ротацией (используется Lumberjack)
	MaxBackups int    `yaml:"max_backups" env:"LOG_MAX_BACKUPS"` // Макс. количество старых файлов лога для хранения (используется Lumberjack)
	MaxAge     int    `yaml:"max_age" env:"LOG_MAX_AGE"`         // Макс. количество дней для хранения файла лога (используется Lumberjack)
	Compress   bool   `yaml:"compress" env:"LOG_COMPRESS"`       // Сжимать ли старые файлы лога при ротации (используется Lumberjack)
	// Format string `yaml:"format"` // Можно добавить выбор форматтера (text/json)
}

// RedisConfig - конфигурация для Redis
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// DatabaseConfig - конфигурация для подключения к базе данных PostgreSQL
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DBName   string `yaml:"dbname"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// ChannelsConfig - размеры буферов для общих каналов
type ChannelsConfig struct {
	OrderBookBufferSize           int `yaml:"order_book_buffer_size"`
	BBOBufferSize                 int `yaml:"bbo_buffer_size"`
	ResyncCommandBufferSize       int `yaml:"resync_command_buffer_size"`
	SubscriptionCommandBufferSize int `yaml:"subscription_command_buffer_size"`
}

// ExchangesConfig - конфигурация для всех бирж
type ExchangesConfig struct {
	Binance BinanceConfig `yaml:"binance"`
	Bybit   BybitConfig   `yaml:"bybit"`
	// Добавьте другие биржи здесь
}

// SpotConfig - специфическая конфигурация для спотового рынка биржи
type SpotConfig struct {
	Enabled                      bool     `yaml:"enabled"`
	WSBaseURL                    string   `yaml:"ws_base_url"`
	RestBaseURL                  string   `yaml:"rest_base_url"`
	TrackedSymbols               []string `yaml:"tracked_symbols"`                    // Символы для отслеживания на этом рынке (первичный список)
	WSMaxTopicsPerSubscribe      int      `yaml:"ws_max_topics_per_subscribe"`        // Лимит топиков в одном WS SUBSCRIBE сообщении
	WSSendMessageRateLimitPerSec int      `yaml:"ws_send_message_rate_limit_per_sec"` // Лимит сообщений в секунду для отправки по WS
}

// Implement MarketConfigInterface for SpotConfig
func (c SpotConfig) IsEnabled() bool                      { return c.Enabled }
func (c SpotConfig) GetWSBaseURL() string                 { return c.WSBaseURL }
func (c SpotConfig) GetRESTBaseURL() string               { return c.RestBaseURL }
func (c SpotConfig) GetWSMaxTopicsPerSubscribe() int      { return c.WSMaxTopicsPerSubscribe }
func (c SpotConfig) GetWSSendMessageRateLimitPerSec() int { return c.WSSendMessageRateLimitPerSec }

// FuturesConfig - специфическая конфигурация для фьючерсного рынка биржи
type FuturesConfig struct {
	Enabled                      bool     `yaml:"enabled"`
	WSBaseURL                    string   `yaml:"ws_base_url"`
	RestBaseURL                  string   `yaml:"rest_base_url"`
	TrackedSymbols               []string `yaml:"tracked_symbols"`                    // Символы для отслеживания на этом рынке (первичный список)
	WSMaxTopicsPerSubscribe      int      `yaml:"ws_max_topics_per_subscribe"`        // Лимит топиков в одном WS SUBSCRIBE сообщении
	WSSendMessageRateLimitPerSec int      `yaml:"ws_send_message_rate_limit_per_sec"` // Лимит сообщений в секунду для отправки по WS
}

// Implement MarketConfigInterface for FuturesConfig
func (c FuturesConfig) IsEnabled() bool                      { return c.Enabled }
func (c FuturesConfig) GetWSBaseURL() string                 { return c.WSBaseURL }
func (c FuturesConfig) GetRESTBaseURL() string               { return c.RestBaseURL }
func (c FuturesConfig) GetWSMaxTopicsPerSubscribe() int      { return c.WSMaxTopicsPerSubscribe }
func (c FuturesConfig) GetWSSendMessageRateLimitPerSec() int { return c.WSSendMessageRateLimitPerSec }

// BinanceConfig - специфическая конфигурация для Binance
type BinanceConfig struct {
	Enabled   bool          `yaml:"enabled"`
	APIKey    string        `yaml:"api_key"`
	APISecret string        `yaml:"api_secret"`
	Spot      SpotConfig    `yaml:"spot"`
	Futures   FuturesConfig `yaml:"futures"`
}

// BybitConfig - специфическая конфигурация для Bybit
type BybitConfig struct {
	Enabled   bool          `yaml:"enabled"`
	APIKey    string        `yaml:"api_key"`
	APISecret string        `yaml:"api_secret"`
	Spot      SpotConfig    `yaml:"spot"`    // Bybit V5 Unified account supports Spot
	Futures   FuturesConfig `yaml:"futures"` // V5 Linear USD-M assumed
}

// ScannerConfig - конфигурация для Scanner'а
type ScannerConfig struct {
	// TrackedSymbols перенесены в MarketConfig Interface и будут получаться оттуда
	// Старое поле TrackedSymbols []string `yaml:"tracked_symbols"` // Удалить или переосмыслить, если символы берутся из MarketConfig
}

// ArbitrageAnalyzerConfig - конфигурация для Arbitrage Analyzer
type ArbitrageAnalyzerConfig struct {
	Enabled       bool          `yaml:"enabled"`
	Symbols       []string      `yaml:"symbols"`   // Symbols for analysis (global list, ideally populated dynamically)
	Exchanges     []string      `yaml:"exchanges"` // List of exchanges/market types for analysis (format "exchange:market_type")
	CheckInterval time.Duration `yaml:"check_interval"`
}

// WebConfig - конфигурация для веб-server
type WebConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ListenAddr string `yaml:"listen_addr"`
	// TODO: Maybe add order book depth limit for web display
}

// RateLimitsConfig - конфигурация для Rate Limiter'ов (если централизованы)
// Эти лимиты могут быть общими для всех REST запросов к бирже, или специфичными для эндпоинтов.
// Проверьте документацию биржи.
type RateLimitsConfig struct {
	BinanceRestPerMin int `yaml:"binance_rest_per_min"` // REST weight limit per minute
	BybitRestPerMin   int `yaml:"bybit_rest_per_min"`   // REST weight limit per minute
	// Add limits for other exchanges or request types
}

// SubscriptionManagerConfig - Конфигурация для SubscriptionManager
type SubscriptionManagerConfig struct {
	Enabled bool `yaml:"enabled"`
	// DefaultSubscriptions теперь содержит MarketType
	DefaultSubscriptions []types.SubscriptionCommand `yaml:"default_subscriptions"`
}

// LoadConfig загружает конфигурацию из YAML файла.
// Возвращает указатель на Config и ошибку.
// Использует стандартный вывод ошибок до инициализации основного логгера.
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		// Используем fmt.Fprintf для вывода ошибок до инициализации основного логгера
		fmt.Fprintf(os.Stderr, "Error reading config file '%s': %v\n", filename, err)
		return nil, fmt.Errorf("failed to read config file '%s': %w", filename, err)
	}

	cfg := &Config{}

	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		// Используем fmt.Fprintf для вывода ошибок до инициализации основного логгера
		fmt.Fprintf(os.Stderr, "Error parsing config file '%s': %v\n", filename, err)
		return nil, fmt.Errorf("failed to parse config file '%s': %w", filename, err)
	}

	return cfg, nil
}

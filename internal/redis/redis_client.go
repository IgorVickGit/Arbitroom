// internal/redis/redis.go
package redis

import (
	"arbitrage-engine/internal/config" // Импортируем пакет config
	"arbitrage-engine/internal/types"
	"context" // Для работы с контекстом
	"encoding/json"
	"fmt"

	// "log" // >>> Удаляем стандартный лог
	"os" // Для fmt.Fprintf
	"time"

	"github.com/go-redis/redis/v8" // Импортируем клиент Redis
	"github.com/sirupsen/logrus"   // >>> Импортируем logrus
)

// RedisClient представляет клиента для взаимодействия с Redis
type RedisClient struct {
	Client *redis.Client
	// Ctx context.Context // Контекст для жизненного цикла клиента (можно убрать, т.к. cancel используется отдельно)
	cancel context.CancelFunc // Функция для отмены контекста при закрытии
	log    *logrus.Entry      // >>> Используем *logrus.Entry для логгера с контекстом компонента
}

// NewRedisClient создает новый экземпляр RedisClient и подключается к Redis
// Принимает структуру конфигурации и логгер logrus.Entry
// *** ОБНОВЛЕННАЯ СИГНАТУРА ***
func NewRedisClient(cfg config.RedisConfig, logger *logrus.Entry) (*RedisClient, error) {
	// Логгируем начало инициализации через переданный логгер
	logger = logger.WithField("redis_addr", cfg.Addr).WithField("redis_db", cfg.DB) // Добавляем контекст адреса и БД к логгеру
	logger.Info("Начало инициализации клиента Redis...")

	// Контекст для *жизненного цикла* клиента (закрытия соединения).
	// Для отдельных операций Redis (Ping, Set, Get и т.д.) будет использоваться контекст, переданный в методы.
	ctx, cancel := context.WithCancel(context.Background())

	// Извлекаем параметры из конфигурации
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		// TODO: Возможно, добавить другие опции из конфига (MinIdleConns, PoolSize и т.д.)
	})

	// Проверяем соединение
	// Используем ctx для Ping, т.к. это первая операция после создания клиента
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second) // Добавляем таймаут на пинг
	defer pingCancel()                                             // Гарантируем отмену контекста для пинга

	pong, err := rdb.Ping(pingCtx).Result()
	if err != nil {
		cancel() // Отменяем контекст жизненного цикла клиента, если соединение не удалось
		// Используем fmt.Fprintf, т.к. это критическая ошибка на этапе инициализации
		fmt.Fprintf(os.Stderr, "FATAL: Не удалось подключиться к Redis по адресу %s: %v\n", cfg.Addr, err)
		// Используем логгер с уровнем Fatal (он вызовет os.Exit)
		logger.WithError(err).WithField("ping_result", pong).Fatal("Ошибка подключения к Redis.")
		// Код ниже недостижим после Fatal, но возвращаем nil и ошибку для сигнатуры
		return nil, fmt.Errorf("не удалось подключиться к Redis по адресу %s: %w", cfg.Addr, err)
	}
	logger.WithField("ping_result", pong).Info("Успешное подключение к Redis.")

	// TODO: Возможно, добавить другие проверки инициализации здесь

	logger.Info("Инициализация клиента Redis завершена.")

	return &RedisClient{
		Client: rdb,
		// Ctx:    ctx, // Не храним контекст операций, передаем его в методы
		cancel: cancel, // Сохраняем функцию отмены
		log:    logger, // >>> Сохраняем переданный логгер *logrus.Entry
	}, nil
}

// Close закрывает соединение с Redis и отменяет контекст жизненного цикла клиента
func (r *RedisClient) Close() error {
	r.log.Info("Закрытие соединения с Redis...") // Используем логгер из структуры
	r.cancel()                                   // Отменяем контекст через неэкспортируемое поле cancel
	err := r.Client.Close()                      // Закрываем клиент Redis
	if err != nil {
		r.log.WithError(err).Error("Ошибка при закрытии соединения Redis.") // Используем логгер из структуры
	}
	r.log.Info("Соединение с Redis закрыто.") // Используем логгер из структуры
	return err
}

// Пример метода: сохранение OrderBookSnapshot в Redis
// Ключ будет формироваться как "orderbook:<source>:<symbol>"
// Значение будет JSON представление OrderBook
// *** ОБНОВЛЕНА СИГНАТУРА для приема контекста ***
func (r *RedisClient) SaveOrderBookSnapshot(ctx context.Context, snapshot *types.OrderBook) error {
	// Используем логгер с контекстом операции
	logger := r.log.WithFields(logrus.Fields{
		"func":   "SaveOrderBookSnapshot",
		"source": snapshot.Source,
		"symbol": snapshot.Symbol,
	})
	logger.Trace("Попытка сохранения OrderBook snapshot в Redis.") // Уровень Trace

	key := fmt.Sprintf("orderbook:%s:%s", snapshot.Source, snapshot.Symbol)

	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		logger.WithError(err).Error("Ошибка сериализации OrderBook snapshot в JSON.") // Используем логгер
		return fmt.Errorf("ошибка сериализации OrderBook snapshot в JSON: %w", err)
	}

	// Используем переданный контекст ctx для операции Set
	err = r.Client.Set(ctx, key, jsonData, 0).Err() // 0 - без TTL
	if err != nil {
		logger.WithError(err).WithField("key", key).Error("Ошибка записи OrderBook snapshot в Redis.") // Используем логгер
		return fmt.Errorf("ошибка записи OrderBook snapshot в Redis (ключ %s): %w", key, err)
	}

	// r.log.WithField("key", key).Trace("Сохранен OrderBook snapshot в Redis.") // Пример детального лога (очень шумно)
	return nil
}

// Пример метода: получение OrderBookSnapshot из Redis
// *** ОБНОВЛЕНА СИГНАТУРА для приема контекста ***
func (r *RedisClient) GetOrderBookSnapshot(ctx context.Context, source string, symbol string) (*types.OrderBook, error) {
	// Используем логгер с контекстом операции
	logger := r.log.WithFields(logrus.Fields{
		"func":   "GetOrderBookSnapshot",
		"source": source,
		"symbol": symbol,
	})
	logger.Trace("Попытка получения OrderBook snapshot из Redis.") // Уровень Trace

	key := fmt.Sprintf("orderbook:%s:%s", source, symbol)

	// Получаем значение по ключу, используя переданный контекст ctx
	jsonData, err := r.Client.Get(ctx, key).Bytes() // Получаем как []byte
	if err == redis.Nil {
		// Ключ не найден - это не ошибка, а ожидаемый результат
		logger.Debug("OrderBook snapshot не найден в Redis.")                                    // Уровень Debug
		return nil, fmt.Errorf("OrderBook snapshot для %s:%s не найден в Redis", source, symbol) // Возвращаем ошибку "не найдено"
	} else if err != nil {
		// Другая ошибка Redis
		logger.WithError(err).WithField("key", key).Error("Ошибка чтения OrderBook snapshot из Redis.") // Используем логгер
		return nil, fmt.Errorf("ошибка чтения OrderBook snapshot из Redis (ключ %s): %w", key, err)
	}

	// Десериализуем JSON в структуру OrderBook
	var snapshot types.OrderBook
	err = json.Unmarshal(jsonData, &snapshot)
	if err != nil {
		logger.WithError(err).WithField("key", key).WithField("data", string(jsonData)).Error("Ошибка десериализации JSON OrderBook snapshot из Redis.") // Используем логгер
		return nil, fmt.Errorf("ошибка десериализации JSON OrderBook snapshot из Redis (ключ %s): %w, data: %s", key, err, string(jsonData))
	}

	logger.Trace("OrderBook snapshot успешно извлечен и распарсен.") // Trace

	return &snapshot, nil
}

// Пример метода: сохранение BBO в Redis
// Ключ будет формироваться как "bbo:<source>:<market_type>:<symbol>" (как в Scanner)
// Значение будет сериализовано/десериализовано бинарно (см. типы.go)
// ИЛИ JSON, если вы используете MarshalBinary на основе JSON
// *** ОБНОВЛЕНА СИГНАТУРА для приема контекста ***
func (r *RedisClient) SaveBBO(ctx context.Context, bbo *types.BBO) error { // >>> Принимаем контекст - FIXED
	// Используем логгер с контекстом операции
	logger := r.log.WithFields(logrus.Fields{
		"func":        "SaveBBO",
		"source":      bbo.Source,
		"market_type": bbo.MarketType, // Добавляем market_type в контекст лога
		"symbol":      bbo.Symbol,
	})
	logger.Trace("Попытка сохранения BBO в Redis.") // Уровень Trace

	// >>> ИСПРАВЛЕНО: Формат ключа соответствует формату в Scanner (включает market_type)
	key := fmt.Sprintf("bbo:%s:%s:%s", bbo.Source, bbo.MarketType, bbo.Symbol)

	// Используем MarshalBinary из types.BBO
	binaryData, err := bbo.MarshalBinary()
	if err != nil {
		logger.WithError(err).Error("Ошибка MarshalBinary для BBO.") // Используем логгер
		return fmt.Errorf("ошибка сериализации BBO: %w", err)
	}

	// Используем переданный контекст ctx для операции Set
	err = r.Client.Set(ctx, key, binaryData, 0).Err() // 0 - без TTL
	if err != nil {
		logger.WithError(err).WithField("key", key).Error("Ошибка записи BBO в Redis.") // Используем логгер
		return fmt.Errorf("ошибка записи BBO в Redis (ключ %s): %w", key, err)
	}

	logger.WithField("key", key).Trace("BBO успешно сохранен в Redis.") // Уровень Trace
	return nil
}

// Пример метода: получение BBO из Redis
// *** ОБНОВЛЕНА СИГНАТУРА для приема контекста ***
func (r *RedisClient) GetBBO(ctx context.Context, source string, marketType types.MarketType, symbol string) (*types.BBO, error) { // >>> Принимаем контекст и marketType - FIXED
	// Используем логгер с контекстом операции
	logger := r.log.WithFields(logrus.Fields{
		"func":        "GetBBO",
		"source":      source,
		"market_type": marketType, // Добавляем market_type в контекст лога
		"symbol":      symbol,
	})
	logger.Trace("Попытка получения BBO из Redis.") // Уровень Trace

	// >>> ИСПРАВЛЕНО: Формат ключа соответствует формату в Scanner (включает market_type)
	key := fmt.Sprintf("bbo:%s:%s:%s", source, marketType, symbol)

	// Используем переданный контекст ctx для операции Get
	binaryData, err := r.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// BBO не найден - это не ошибка, а ожидаемый результат
		logger.Debug("BBO не найден в Redis.") // Уровень Debug
		// Важно: в Scanner метод GetBBO обрабатывает goredis.Nil как "не найдено",
		// и возвращает nil без ошибки. Здесь мы также должны вернуть nil без ошибки,
		// если ключ не найден, чтобы Scanner мог корректно обработать этот случай.
		return nil, nil // >>> Возвращаем nil, nil при redis.Nil - FIXED
	} else if err != nil {
		// Другая ошибка Redis
		logger.WithError(err).WithField("key", key).Error("Ошибка чтения BBO из Redis.") // Используем логгер
		return nil, fmt.Errorf("ошибка чтения BBO из Redis (ключ %s): %w", key, err)
	}

	var bbo types.BBO
	// Используем UnmarshalBinary из types.BBO
	err = bbo.UnmarshalBinary(binaryData)
	if err != nil {
		// Ошибка десериализации данных
		logger.WithError(err).WithField("key", key).WithField("data", string(binaryData)).Error("Ошибка десериализации BBO из Redis.") // Используем логгер
		return nil, fmt.Errorf("ошибка десериализации BBO из Redis (ключ %s): %w, data: %s", key, err, string(binaryData))
	}

	logger.Trace("BBO успешно извлечен из Redis.") // Trace

	return &bbo, nil
}

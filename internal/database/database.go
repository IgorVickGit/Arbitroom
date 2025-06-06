// internal/database/database.go
package database

import (
	"context"
	"database/sql"
	"fmt"

	// "log" // >>> Удаляем стандартный лог
	"os" // Для fmt.Fprintf
	"time"

	_ "github.com/lib/pq" // Импорт драйвера PostgreSQL

	"arbitrage-engine/internal/config" // Убедитесь, что путь к вашему конфигу правильный

	"github.com/sirupsen/logrus" // >>> Импортируем logrus
)

// Database структура хранит подключение к БД и логгер
type Database struct {
	DB  *sql.DB
	log *logrus.Entry // >>> Используем *logrus.Entry для логгера с контекстом компонента
}

// NewDatabase устанавливает новое подключение к базе данных PostgreSQL
// Принимает конфигурацию и логгер logrus.Entry
// *** ОБНОВЛЕННАЯ СИГНАТУРА ***
func NewDatabase(cfg config.DatabaseConfig, logger *logrus.Entry) (*Database, error) {
	// Логгируем начало инициализации через переданный логгер, добавляя контекст БД
	logger = logger.WithFields(logrus.Fields{
		"db_host": cfg.Host,
		"db_port": cfg.Port,
		"db_name": cfg.DBName,
		"db_user": cfg.User,
	})
	logger.Info("Начало инициализации подключения к базе данных...")

	// Строка подключения. Пароль не включаем в лог.
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode)
	// logger.Debugf("Строка подключения к БД (без пароля): %s", connStr) // Опционально логировать строку без пароля на Debug

	// Открываем соединение
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		// Используем переданный логгер с уровнем Error
		logger.WithError(err).Error("Ошибка при открытии соединения с БД.")
		// Для критической ошибки на этапе инициализации, можно также использовать fmt.Fprintf
		fmt.Fprintf(os.Stderr, "FATAL: Ошибка при открытии соединения с БД: %v\n", err)
		// Или даже logger.Fatal, если приложение не может работать без БД.
		// logger.WithError(err).Fatal("Не удалось открыть соединение с БД.")
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Проверяем соединение (Ping)
	// Используем контекст с таймаутом для проверки пинга
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		// Используем переданный логгер с уровнем Error
		logger.WithError(err).Error("Ошибка при проверке соединения с БД (ping).")
		// Важно закрыть соединение, даже если пинг не удался
		// Используем логгер перед закрытием
		logger.Info("Закрытие соединения с БД из-за ошибки пинга.")
		db.Close()
		// Для критической ошибки на этапе инициализации, можно также использовать fmt.Fprintf
		fmt.Fprintf(os.Stderr, "FATAL: Ошибка при проверке соединения с БД (ping): %v\n", err)
		// Или даже logger.Fatal, если приложение не может работать без БД.
		// logger.WithError(err).Fatal("Не удалось проверить соединение с БД.")
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Используем переданный логгер с уровнем Info
	logger.Info("Соединение с базой данных успешно установлено и проверено.")

	// Опционально: Настроить пул соединений (логируем, если нужно)
	logger.WithFields(logrus.Fields{
		"max_open_conns":    25,
		"max_idle_conns":    25,
		"conn_max_lifetime": 5 * time.Minute,
	}).Debug("Настройка пула соединений БД.") // Уровень Debug для деталей конфигурации
	db.SetMaxOpenConns(25)                 // Максимальное количество открытых соединений
	db.SetMaxIdleConns(25)                 // Максимальное количество бездействующих соединений
	db.SetConnMaxLifetime(5 * time.Minute) // Время жизни соединения

	logger.Info("Инициализация клиента базы данных завершена.")

	return &Database{DB: db, log: logger}, nil // >>> Сохраняем логгер logrus.Entry в структуре
}

// Close закрывает соединение с базой данных
func (d *Database) Close() {
	// Используем логгер из структуры
	if d != nil && d.DB != nil { // Добавлена проверка на nil, чтобы избежать паники при d == nil
		d.log.Info("Закрытие соединения с базой данных...")
		err := d.DB.Close()
		if err != nil {
			d.log.WithError(err).Error("Ошибка при закрытии соединения с БД.") // Используем логгер из структуры
		}
		d.log.Info("Соединение с базой данных закрыто.") // Используем логгер из структуры
	} else {
		// Если Close вызван на nil Database или DB, просто логируем это (опционально)
		// logrus.Warn("Попытка закрыть nil клиента базы данных или соединение.") // Можно использовать logrus напрямую здесь
	}
}

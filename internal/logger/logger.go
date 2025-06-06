// internal/logger/logger.go
package logger

import (
	"fmt"
	"os"
	"strings"

	"arbitrage-engine/internal/config" // Импортируем вашу конфигурацию

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2" // Для ротации файлов логов
)

// Log экспортируемая переменная - это наш настроенный логгер
var Log *logrus.Logger

// InitLogger инициализирует глобальный логгер на основе конфигурации LogConfig.
// Эту функцию нужно вызвать ОДИН раз в самом начале main().
func InitLogger(cfg *config.Config) error {
	Log = logrus.New()

	// 1. Установка уровня логирования
	level, err := logrus.ParseLevel(strings.ToLower(cfg.Log.Level))
	if err != nil {
		Log.SetLevel(logrus.InfoLevel)
		fmt.Fprintf(os.Stderr, "Некорректный уровень логирования '%s' в конфиге. Установлен уровень Info. Ошибка: %v\n", cfg.Log.Level, err)
	} else {
		Log.SetLevel(level)
	}

	// 2. Настройка форматтеров (разные для консоли и файла)
	// Форматтер для консоли (с цветами)
	consoleFormatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		ForceColors:     true, // Включаем цвета принудительно для консоли
		DisableColors:   false,
	}

	// Форматтер для файла (без цветов)
	fileFormatter := &logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		ForceColors:     false, // ОТКЛЮЧАЕМ цвета для файла
		DisableColors:   true,  // Явно отключаем цвета
	}

	// 3. Настройка вывода (либо в файл, либо в консоль)
	// Удалена неиспользуемая переменная writers.

	if cfg.Log.FilePath != "" {
		// Конфигурация для записи в файл
		fileLogger := &lumberjack.Logger{
			Filename:   cfg.Log.FilePath,
			MaxSize:    cfg.Log.MaxSize,
			MaxBackups: cfg.Log.MaxBackups,
			MaxAge:     cfg.Log.MaxAge,
			Compress:   cfg.Log.Compress,
		}
		Log.SetOutput(fileLogger)       // Пишем только в файл
		Log.SetFormatter(fileFormatter) // Используем форматтер без цветов для файла
		Log.Infof("Логи будут записываться в файл: %s (без цветов)", cfg.Log.FilePath)

	} else {
		// Конфигурация для записи в консоль (если файл не указан)
		Log.SetOutput(os.Stderr)           // Пишем в стандартный поток ошибок
		Log.SetFormatter(consoleFormatter) // Используем форматтер с цветами для консоли
		Log.Info("Запись логов в файл не настроена. Логи будут выводиться в stderr (с цветами).")
	}

	// 4. Дополнительные настройки (опционально)
	// Log.SetReportCaller(true) // Добавлять информацию о файле и строке вызова (может иметь небольшой оверхед)

	// Выводим сообщение об успешной инициализации уже через настроенный логгер.
	// Используем простой лог, чтобы избежать рекурсии, если логирование упало.
	// Или используем fmt.Println для самых первых сообщений.
	// fmt.Fprintf(os.Stderr, "Logging initialized. Level: %s, File: %s\n", Log.GetLevel().String(), cfg.Log.FilePath) // Уже делается в main.go

	return nil // Возвращаем nil при успешной инициализации
}

// IsDebugEnabled - хелпер для проверки, включен ли уровень Debug или Trace.
// Используется перед выполнением ресурсоемких операций, результаты которых
// нужны только для Debug/Trace логов.
func IsDebugEnabled() bool {
	if Log == nil {
		// Логгер еще не инициализирован, считаем, что debug не включен
		return false
	}
	return Log.GetLevel() >= logrus.DebugLevel
}

// IsTraceEnabled - хелпер для проверки, включен ли уровень Trace.
func IsTraceEnabled() bool {
	if Log == nil {
		return false
	}
	return Log.GetLevel() >= logrus.TraceLevel
}

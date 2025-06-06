// internal/subscription/manager.go

package subscription

import (
	"arbitrage-engine/internal/config"
	"arbitrage-engine/internal/types"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus" // >>> Импортируем logrus
)

// SubscriptionManager управляет подписками WebSocket для всех бирж.
// Принимает команды на подписку/отписку и отправляет их соответствующим клиентам.
type SubscriptionManager struct {
	ctx context.Context
	log *logrus.Entry                    // >>> Используем *logrus.Entry
	cfg config.SubscriptionManagerConfig // Конфигурация менеджера

	// Мапа каналов для отправки команд подписки конкретным биржевым клиентам.
	// Ключ: "source:market_type" (строка), Значение: канал команд подписки для этой биржи/рынка.
	clientSubChannels map[string]chan *types.SubscriptionCommand // Принимает указатели
}

// NewSubscriptionManager создает новый экземпляр SubscriptionManager.
// Принимает контекст, логгер (logrus.Entry), конфигурацию и мапу каналов команд клиентов.
// *** ОБНОВЛЕННАЯ СИГНАТУРА ***
func NewSubscriptionManager(
	ctx context.Context,
	logger *logrus.Entry, // >>> Принимаем logrus logger
	cfg config.SubscriptionManagerConfig,
	clientSubChannels map[string]chan *types.SubscriptionCommand,
) *SubscriptionManager {
	// Создаем логгер с контекстом для данного компонента
	logger = logger.WithField("component", "subscription_manager")
	logger.Info("Начало инициализации SubscriptionManager...") // Используем logrus logger

	manager := &SubscriptionManager{
		ctx:               ctx,
		log:               logger, // >>> Сохраняем logrus logger
		cfg:               cfg,
		clientSubChannels: clientSubChannels,
	}

	// Логика отправки команд подписки по умолчанию будет в Run()

	logger.Info("Инициализация SubscriptionManager завершена.") // Используем logrus logger

	return manager
}

// Run запускает основной цикл работы SubscriptionManager.
// Он отправляет начальные команды подписки и ждет сигнала отмены контекста.
func (m *SubscriptionManager) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done() // Уменьшаем счетчик WaitGroup при выходе из горутины

	m.log.Info("Subscription Manager запущен.")          // Используем logrus logger
	defer m.log.Info("Subscription Manager остановлен.") // Используем logrus logger

	// --- Step 1: Send initial default subscriptions from config ---
	// This part executes ONCE when the Run goroutine starts.
	if m.cfg.Enabled { // Check if the manager is enabled in config
		m.log.Infof("SubscriptionManager: Отправка %d начальных команд подписки из конфига...", len(m.cfg.DefaultSubscriptions)) // Используем logrus logger
		// Iterate through the commands list from config DefaultSubscriptions.
		for i, subCmd := range m.cfg.DefaultSubscriptions { // subCmd is a types.SubscriptionCommand (value copy)
			// Check context before sending each command.
			select {
			case <-m.ctx.Done(): // If context is cancelled during initial subscription sending...
				m.log.Info("SubscriptionManager: Отмена контекста во время отправки начальных подписок, прерывание.") // Используем logrus logger
				return                                                                                                // Exit Run goroutine. Defer will execute.
			default:
				// Context is active, continue.
			}

			// Determine the client key "source:market_type" to find the correct channel.
			// Use the Source and MarketType fields from the command itself to form this key.
			clientKey := fmt.Sprintf("%s:%s", subCmd.Source, subCmd.MarketType) // *** USE Source and MarketType FROM COMMAND ***

			// Find the channel for the corresponding exchange:market_type in the map.
			clientChan, ok := m.clientSubChannels[clientKey] // *** USE clientKey ***
			if !ok {
				// Логируем предупреждение, если клиент не найден для начальной подписки.
				m.log.WithFields(logrus.Fields{
					"client_key": clientKey,
					"command":    subCmd,
				}).Warn("SubscriptionManager: Пропускаю начальную подписку для неизвестного клиента (комбинация source:market_type не найдена в мапе каналов).") // Используем logrus logger Warn
				continue // Skip to the next command.
			}

			// Send the command to the client's channel.
			// Send a pointer to a COPY of the command, as the range variable 'subCmd' is reused.
			cmdToSend := subCmd // Create a copy of the command struct
			// Логируем отправку команды
			m.log.WithFields(logrus.Fields{
				"command_type":  cmdToSend.Type,
				"client_key":    clientKey,
				"symbol":        cmdToSend.Symbol,
				"depth":         cmdToSend.Depth,
				"command_index": i,
			}).Info("SubscriptionManager: Отправляю начальную команду подписки...") // Используем logrus logger Info

			// Use a timeout and context for sending to prevent blocking.
			// This timeout applies to the attempt to send into the channel.
			// TODO: Externalize this send timeout to config.
			sendCtx, cancelSend := context.WithTimeout(m.ctx, 5*time.Second) // Timeout for sending into channel
			defer cancelSend()                                               // Ensure the send context is cancelled

			select {
			case clientChan <- &cmdToSend: // Send pointer to the COPY of the command
				// Логируем успешную отправку
				m.log.WithFields(logrus.Fields{
					"command_type": cmdToSend.Type,
					"client_key":   clientKey,
					"symbol":       cmdToSend.Symbol,
				}).Debug("SubscriptionManager: Начальная команда успешно отправлена.") // Используем logrus logger Debug
				// Successfully sent, continue loop for the next command.
			case <-sendCtx.Done(): // Send context or manager context cancelled during channel wait.
				// Логируем ошибку отправки по таймауту или отмене контекста.
				m.log.WithFields(logrus.Fields{
					"command_type":  cmdToSend.Type,
					"client_key":    clientKey,
					"symbol":        cmdToSend.Symbol,
					"context_error": sendCtx.Err(), // Логируем ошибку контекста
				}).Warn("SubscriptionManager: Отправка начальной команды прервана отменой контекста во время ожидания канала.") // Используем logrus logger Warn
				// If a core client channel is full at startup, it indicates a serious config/sizing issue.
				// We can log and continue for other commands, or exit Run. Let's log and continue.
			}
			// The defer cancelSend() will be called here before the next loop iteration or before Run exits.
		}
		m.log.Println("SubscriptionManager: Отправка всех начальных команд подписки завершена.") // Используем logrus logger
	} else {
		m.log.Println("SubscriptionManager: Менеджер подписок отключен в конфиге, начальные подписки не отправляются.") // Используем logrus logger
	}

	// --- Step 2: Wait for context cancellation ---
	// If you have a loop for processing INCOMING subscription management commands
	// (e.g., from Arbitrage Analyzer dynamically changing subscriptions),
	// this <-m.ctx.Done() should be inside that loop (in one select statement).
	// In the current implementation, there is no such loop, so we just wait for context cancellation here.
	m.log.Info("Subscription Manager: Ожидание сигнала отмены контекста...") // Используем logrus logger
	<-m.ctx.Done()                                                           // Goroutine blocks here until context is cancelled.

	m.log.Info("Subscription Manager: Получен сигнал отмены контекста.") // Используем logrus logger

	// TODO: On context cancellation, potentially send unsubscribe commands to all clients?
	// This is hard to do reliably as clients might already be shutting down.
	// It's usually sufficient for clients to close WS connections themselves
	// upon receiving their clientCtx cancellation signal, which automatically
	// cancels subscriptions on the exchange side.

}

// SendCommand отправляет команду управления подпиской конкретному биржевому клиенту.
// Это публичный метод, вызываемый другими компонентами (например, Arbitrage Analyzer),
// когда нужно динамически изменить подписку (например, запросить большую глубину).
// Он принимает значение команды (не указатель), создает его копию при отправке в канал.
// Использует logrus.
func (m *SubscriptionManager) SendCommand(cmd types.SubscriptionCommand) error {
	// Используем логгер с контекстом для этого метода
	logger := m.log.WithField("func", "SendCommand")
	logger.WithField("command", cmd).Debug("SubscriptionManager: Получена команда для отправки.") // Используем logrus logger Debug

	// Check that the command is not empty and has source and symbol.
	if cmd.Source == "" || cmd.Symbol == "" || cmd.MarketType == "" { // Check MarketType too
		// Логируем ошибку некорректной команды
		logger.WithField("command", cmd).Error("SubscriptionManager: Получена некорректная команда без Source, MarketType или Symbol.") // Используем logrus logger Error
		return fmt.Errorf("некорректная команда подписки: отсутствует Source, MarketType или Symbol")
	}

	// Determine the client key "source:market_type" using fields from the command.
	clientKey := fmt.Sprintf("%s:%s", cmd.Source, cmd.MarketType) // *** USE Source and MarketType FROM COMMAND ***

	// Find the channel for the corresponding exchange:market_type in the map.
	channel, ok := m.clientSubChannels[clientKey] // *** USE clientKey ***
	if !ok {
		// Логируем предупреждение, если клиент не найден для команды.
		logger.WithField("client_key", clientKey).WithField("command", cmd).Warn("SubscriptionManager: Получена команда для неизвестного клиента (комбинация source:market_type не найдена в мапе каналов). Игнорирую.") // Используем logrus logger Warn
		return fmt.Errorf("неизвестный клиент для команды подписки: %s:%s", cmd.Source, cmd.MarketType)
	}

	// Send the command to the client's channel.
	// Use a select with a timeout and context check to prevent blocking the sender
	// if the client's channel is full or the application is shutting down.
	// Логируем попытку отправки команды
	logger.WithFields(logrus.Fields{
		"command_type": cmd.Type,
		"client_key":   clientKey,
		"symbol":       cmd.Symbol,
		"depth":        cmd.Depth,
	}).Info("SubscriptionManager: Отправляю команду...") // Используем logrus logger Info

	// Use the manager's context (m.ctx) for interruption.
	// Create a short context with a timeout for the send operation itself.
	// This timeout should be less than the client's channel read timeout.
	// TODO: Externalize this send timeout to config.
	sendCtx, cancel := context.WithTimeout(m.ctx, 5*time.Second) // Timeout for sending into channel
	defer cancel()                                               // Ensure the context is cancelled after use

	select {
	case channel <- &cmd: // Send pointer to the VALUE copy 'cmd'
		// Логируем успешную отправку
		logger.WithFields(logrus.Fields{
			"command_type": cmd.Type,
			"client_key":   clientKey,
			"symbol":       cmd.Symbol,
		}).Debug("SubscriptionManager: Команда успешно отправлена.") // Используем logrus logger Debug
		return nil // Successfully sent.
	case <-sendCtx.Done(): // Send context or manager context cancelled.
		// Логируем ошибку отправки по таймауту или отмене контекста.
		logger.WithFields(logrus.Fields{
			"command_type":  cmd.Type,
			"client_key":    clientKey,
			"symbol":        cmd.Symbol,
			"context_error": sendCtx.Err(), // Логируем ошибку контекста
		}).Warn("SubscriptionManager: Отправка команды прервана отменой контекста или таймаутом.") // Используем logrus logger Warn
		// Check why context was cancelled.
		if sendCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("таймаут отправки команды подписки клиенту %s:%s", cmd.Source, cmd.MarketType)
		} else { // Manager context was cancelled (SIGINT/SIGTERM).
			return m.ctx.Err() // Return context error.
		}
	}
}

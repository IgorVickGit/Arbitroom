// internal/web/server.go
package web

import (
	"context"
	"encoding/json" // Added for string formatting like "source:market_type"

	// "log" // >>> Удаляем стандартный лог
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"arbitrage-engine/internal/scanner" // Make sure scanner is imported
	"arbitrage-engine/internal/types"   // Make sure types is imported

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus" // >>> Импортируем logrus
)

// TODO: Конфигурация для веб-сервера и WebSocket (буферы, таймауты, Origin) должна быть в конфиге приложения.

// WebSocketServer управляет WebSocket соединениями и рассылкой данных
type WebSocketServer struct {
	scanner *scanner.Scanner // Ссылка на Scanner для получения данных из Redis

	clients map[*websocket.Conn]bool // Реестр активных клиентов (мьютекс защищает доступ к мапе)
	mu      sync.Mutex               // Мьютекс для защиты реестра клиентов

	// Upgrader для HTTP -> WebSocket
	upgrader websocket.Upgrader

	// Контекст и WaitGroup из main.go для управления внутренними горутинами
	// Эти поля используются в HandleWebSocket для запуска clientReader/clientWriter горутин.
	serverCtx context.Context // Главный контекст приложения
	serverWg  *sync.WaitGroup // WaitGroup из main.go
	// serverCancel context.CancelFunc // Not used in current StartServer logic

	log *logrus.Entry // >>> Используем *logrus.Entry для логгера с контекстом компонента
}

// NewWebSocketServer создает новый экземпляр WebSocketServer.
// Принимает контекст, WaitGroup из main.go, Scanner и логгер (logrus.Entry).
// *** ОБНОВЛЕННАЯ СИГНАТУРА для приема логгера ***
func NewWebSocketServer(ctx context.Context, wg *sync.WaitGroup, s *scanner.Scanner, logger *logrus.Entry) *WebSocketServer {
	// Создаем логгер с контекстом для данного компонента
	logger = logger.WithField("component", "web_server")
	logger.Info("Начало инициализации WebSocketServer...") // Используем logrus logger

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024, // TODO: Из конфига
		WriteBufferSize: 1024, // TODO: Из конфига
		CheckOrigin: func(r *http.Request) bool {
			// TODO: В продакшене проверять Origin!
			// logrus logger недоступен напрямую внутри этой анонимной функции CheckOrigin.
			// Можно захватить logger из внешней области видимости, если нужно логировать здесь.
			// logger.WithField("origin", r.Header.Get("Origin")).Debug("Проверка Origin..."); // Пример использования захваченного логгера
			return true // Allow any origin for example
		},
	}

	server := &WebSocketServer{
		scanner:   s,
		clients:   make(map[*websocket.Conn]bool),
		upgrader:  upgrader,
		serverCtx: ctx,    // Save main context
		serverWg:  wg,     // Save WaitGroup from main
		log:       logger, // >>> Сохраняем logrus logger
	}

	logger.Info("Инициализация WebSocketServer завершена.") // Используем logrus logger

	return server
}

// HandleWebSocket устанавливает WebSocket соединение и запускает reader/writer горутины для этого клиента.
// Эти горутины добавляются к main WaitGroup.
// Вызывается Gin роутером. Gin Context не используется напрямую для логирования здесь.
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер ***
func (ws *WebSocketServer) HandleWebSocket(c *gin.Context) {
	// Логируем начало обработки нового соединения
	ws.log.Info("HandleWebSocket: Попытка установить WebSocket соединение...")

	conn, err := ws.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		// Логируем ошибку апгрейда с уровнем Error
		ws.log.WithError(err).Error("HandleWebSocket: Ошибка апгрейда HTTP до WebSocket.") // Используем logrus logger Error
		// Gin уже отправит ответ с ошибкой клиенту.
		return
	}

	ws.addClient(conn) // Add client to registry (under mutex). addClient использует logrus.

	// Логируем успешное подключение
	ws.log.Infof("HandleWebSocket: Новый WebSocket client подключен: %s. Всего клиентов: %d", conn.RemoteAddr(), ws.countClients()) // Используем logrus logger Infof

	// Launch reader and writer goroutines for each client.
	// Both goroutines must be added to the main WaitGroup and listen to context.
	// Передаем контекст main приложения (ws.serverCtx) и WaitGroup main приложения (ws.serverWg)

	// Add reader goroutine to main WaitGroup
	ws.serverWg.Add(1)
	go ws.clientReader(conn, ws.serverCtx, ws.serverWg) // Pass serverCtx and serverWg

	// Add writer goroutine to main WaitGroup
	ws.serverWg.Add(1)
	go ws.clientWriter(conn, ws.serverCtx, ws.serverWg) // Pass serverCtx and serverWg

	// HandleWebSocket finishes quickly, its goroutine is already accounted for in main.go (wg.Add(1) before go StartServer).
	// No defer wg.Done() here.
}

// addClient adds a client to the registry (with locking). Uses logrus.
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер ***
func (ws *WebSocketServer) addClient(conn *websocket.Conn) {
	ws.mu.Lock()
	ws.clients[conn] = true
	ws.mu.Unlock()
	// Логируем добавление клиента на уровне Debug
	ws.log.WithField("remote_addr", conn.RemoteAddr()).Debug("Клиент добавлен в реестр.") // Используем logrus logger Debug
}

// removeClient removes a client from the registry and closes the connection gracefully.
// This function should be called only once per connection (from defer in reader OR writer).
// Ensures the connection is closed and removed from the map. Uses logrus.
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер ***
func (ws *WebSocketServer) removeClient(conn *websocket.Conn) {
	ws.mu.Lock()
	// Проверяем, существует ли клиент в мапе перед удалением
	if _, ok := ws.clients[conn]; !ok {
		ws.mu.Unlock() // Освобождаем мьютекс
		// Клиент уже удален (например, обработан другой горутиной removeClient)
		ws.log.WithField("remote_addr", conn.RemoteAddr()).Debug("Попытка удалить клиента, который уже удален.") // Используем logrus logger Debug
		return
	}
	delete(ws.clients, conn) // Remove from map
	ws.mu.Unlock()           // Unlock map

	// Логируем начало удаления клиента
	ws.log.WithField("remote_addr", conn.RemoteAddr()).Info("Удаление клиента из реестра и закрытие соединения...") // Используем logrus logger Info

	// Try sending a normal closure message (CloseMessage).
	closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down")
	writeTimeout := 1 * time.Second // Timeout for writing a control frame (TODO: From config)

	// Send a close control frame. WriteControl uses only deadline.
	// Use time.Now().Add(writeTimeout) as Deadline for the write operation itself.
	// Логируем попытку отправки CloseMessage на уровне Debug
	ws.log.WithField("remote_addr", conn.RemoteAddr()).Debug("Попытка отправки CloseMessage.")
	err := conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(writeTimeout)) // Используем conn
	if err != nil && err != websocket.ErrCloseSent && err != http.ErrServerClosed {
		// Логируем ошибку отправки CloseMessage на уровне Warn
		ws.log.WithError(err).WithField("remote_addr", conn.RemoteAddr()).Warn("Ошибка отправки CloseMessage.") // Используем logrus logger Warn
	} else if err == nil {
		ws.log.WithField("remote_addr", conn.RemoteAddr()).Debug("CloseMessage успешно отправлен.") // Используем logrus logger Debug
	}

	// Perform physical connection closure.
	err = conn.Close() // Используем conn
	if err != nil {
		// Ignore "use of closed network connection" errors as they can happen
		// if the connection was closed concurrently by a read error.
		if !strings.Contains(err.Error(), "use of closed network connection") {
			ws.log.WithError(err).WithField("remote_addr", conn.RemoteAddr()).Error("Ошибка закрытия WebSocket соединения.") // Используем logrus logger Error
		} else {
			ws.log.WithField("remote_addr", conn.RemoteAddr()).Trace("Ошибка 'use of closed network connection' при закрытии WS.") // Trace
		}
	}

	ws.log.WithField("remote_addr", conn.RemoteAddr()).Infof("WebSocket client отключен и удален. Всего клиентов: %d", ws.countClients()) // Используем logrus logger Infof
}

// countClients safely returns the number of active clients. Uses logrus.
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер (опционально для логирования в теле) ***
func (ws *WebSocketServer) countClients() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	count := len(ws.clients)
	// ws.log.Tracef("Текущее количество клиентов: %d", count); // Optional Trace log
	return count
}

// clientReader reads messages from the client. Terminates on read error or context cancellation. Uses logrus.
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер ***
func (ws *WebSocketServer) clientReader(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup) {
	// wg.Add(1) is already called in HandleWebSocket before launching this goroutine.
	defer wg.Done()             // Notify main WaitGroup on exit
	defer ws.removeClient(conn) // Ensure client removal and connection closure

	// Логгер для этой горутины с контекстом
	logger := ws.log.WithField("goroutine", "client_reader").WithField("remote_addr", conn.RemoteAddr())
	logger.Info("Launched.")       // Используем logrus logger Info
	defer logger.Info("Finished.") // Используем logrus logger Info

	// Set read timeout. If no activity (pings) from client for long, ReadMessage will return error.
	// TODO: Get ReadTimeout for web sockets from config.
	readTimeout := time.Duration(30 * time.Second) // Example read timeout for web client

	for {
		select {
		case <-ctx.Done(): // Listen to context cancellation signal (main app context)
			logger.Info("Received context cancellation signal, exiting.") // Используем logrus logger Info
			return                                                        // Exit goroutine

		default:
			// Set ReadDeadline before each read.
			conn.SetReadDeadline(time.Now().Add(readTimeout)) // Используем conn

			// Read the next message. ReadMessage is blocking.
			// Ignore variable message as we don't process client messages yet.
			messageType, _, err := conn.ReadMessage() // Используем conn

			// Reset ReadDeadline after reading (even if there was an error).
			conn.SetReadDeadline(time.Time{}) // Zero time means no deadline, Используем conn

			if err != nil {
				// Handle read errors (including timeout, normal/unexpected close, context cancellation)
				// Логируем ошибки чтения с уровнем Warn или Error
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					logger.WithError(err).Warn("Unexpected WebSocket closed.") // Используем logrus logger Warn
				} else if err == context.Canceled {
					logger.Info("Read cancelled by context.") // Используем logrus logger Info
				} else if _, ok := err.(net.Error); ok && strings.Contains(err.Error(), "use of closed network connection") {
					// "use of closed network connection" error means the connection was closed by another goroutine (e.g., writer or removeClient)
					logger.Debug("Read interrupted due to closed network connection.") // Используем logrus logger Debug
				} else {
					logger.WithError(err).Error("Read error.") // Используем logrus logger Error
				}
				// On any read error (including timeout or close), assume goroutine should terminate.
				return // Exit reader goroutine on error or close. Defer ws.removeClient(conn) will be called.
			}

			// Process messages received from the client (if needed).
			// E.g., if client sends subscribe commands or requests.
			if messageType == websocket.PingMessage {
				// Gorilla WS automatically handles PING by responding with PONG. This block might be unnecessary but harmless.
				// logger.Trace("Received PING"); // Optional Trace log
			} else if messageType == websocket.CloseMessage {
				logger.Info("Client requested close.") // Используем logrus logger Info
				// If client requested close, we exit. removeClient(conn) in defer will close the connection from our side.
				return
			} else if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				// Handle text or binary messages from the client (e.g., commands from web UI)
				logger.WithField("message_type", messageType).Trace("Received message from client.") // Optional Trace log
				// TODO: Implement logic to process commands from the client (e.g., change symbols, depth limit)
			}
			// Select/for loop continues for the next message.
		}
	}
}

// clientWriter periodically sends order book and BBO data to the client. Terminates on write error or context cancellation. Uses logrus.
// *** MODIFIED: Fetches and sends data with MarketType consideration ***
// *** ИСПОЛЬЗУЕТ сохраненный logrus логгер ***
func (ws *WebSocketServer) clientWriter(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup) {
	// wg.Add(1) is already called in HandleWebSocket before launching this goroutine.
	defer wg.Done()             // Notify main WaitGroup on exit
	defer ws.removeClient(conn) // Ensure client removal and connection closure

	// Логгер для этой горутины с контекстом
	logger := ws.log.WithField("goroutine", "client_writer").WithField("remote_addr", conn.RemoteAddr())
	logger.Info("Launched.")       // Используем logrus logger Info
	defer logger.Info("Finished.") // Используем logrus logger Info

	// TODO: Get list of markets (exchange:market_type) and symbols to display from config,
	// or even better, get the list of currently tracked markets/symbols from the Scanner's metadata.

	// Using example lists based on config structure and Scanner's tracked symbols.
	// A more robust way would be to get the list of *active* metadata keys from Scanner
	// (e.g. s.GetActiveMetadataKeys() []string returning "source:market_type:symbol")
	// and then process those specific keys.

	// Hardcoded list of source:market_type combinations to iterate through for simplicity in this example.
	// A real implementation would read this from config or query DB/Scanner for active markets.
	sourceMarketCombinations := []string{
		"binance:spot",
		"binance:linear_futures",
		"bybit:spot",           // Added Bybit Spot based on config structure
		"bybit:linear_futures", // Assuming Bybit linear futures is enabled
		// TODO: Add other enabled combinations from config
	}

	// Get the map of globally tracked symbols from Scanner using the new public method.
	trackedSymbolsMap := ws.scanner.GetTrackedSymbols() // *** ИСПОЛЬЗУЕТ НОВЫЙ ПУБЛИЧНЫЙ МЕТОД ***
	if len(trackedSymbolsMap) == 0 {
		logger.Warn("Scanner has no tracked symbols. Cannot send market data.") // Используем logrus logger Warn
		// Could potentially wait or retry if symbols are added later, but for now, exit.
		// If symbols are added later, this goroutine would need a signal to retry fetching the list.
		return // Exit writer goroutine
	}

	orderBookLimit := 10 // TODO: From config or client request

	// Use a ticker for periodic data sending (pull model)
	ticker := time.NewTicker(500 * time.Millisecond) // Data update frequency (TODO: From config)
	defer ticker.Stop()

	// TODO: Use a channel to receive signals about NEW DATA from Scanner (push model)
	// This is more efficient than constantly polling Redis with a ticker.

	for {
		select {
		case <-ticker.C: // Triggered by ticker interval
			// Time to collect data and send it to the client.
			webData := types.WebDataMessage{
				Type: "market_data_update", // Generic type for combined data
				// Initialize the deeply nested map
				Data: make(map[string]map[string]map[types.MarketType]types.WebExchangeData),
			}

			// Collect data for all relevant market type + symbol combinations.
			// Iterate through configured/relevant Source -> MarketType combinations
			for _, sourceMarketKey := range sourceMarketCombinations {
				parts := strings.Split(sourceMarketKey, ":")
				if len(parts) != 2 {
					logger.WithField("source_market_key", sourceMarketKey).Warn("Invalid source:market_type format. Skipping.") // Используем logrus logger Warn
					continue
				}
				source := parts[0]
				marketTypeStr := parts[1]
				marketType := types.MarketType(marketTypeStr) // Convert string to MarketType

				// Initialize map for this source if it doesn't exist
				if _, ok := webData.Data[source]; !ok {
					webData.Data[source] = make(map[string]map[types.MarketType]types.WebExchangeData)
				}

				// Now iterate through symbols that Scanner is tracking
				for symbol := range trackedSymbolsMap { // Iterate through the map keys (uppercase symbols)
					// Initialize map for this symbol under the source if it doesn't exist
					if _, ok := webData.Data[source][symbol]; !ok {
						webData.Data[source][symbol] = make(map[types.MarketType]types.WebExchangeData)
					}

					// --- Fetch data for this specific Source, MarketType, Symbol ---
					// Use the writer's context (ctx) when requesting data from Scanner.
					// Scanner methods (GetBBO, GetOrderBookLevels, GetSyncMetadata) now accept MarketType.

					// Get BBO from Redis (via Scanner)
					// Scanner.GetBBO now accepts context.
					bbo, err := ws.scanner.GetBBO(ctx, source, marketType, symbol) // *** PASS CTX AND MARKETTYPE ***
					if err != nil {
						// Error getting BBO (not "not found" or context.Canceled) was logged in Scanner. Continue collecting other data.
						logger.WithError(err).WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
						}).Debug("Error getting BBO from scanner.") // Используем logrus logger Debug
					} else if bbo != nil {
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
							"bid": bbo.BestBid, "ask": bbo.BestAsk,
						}).Trace("Fetched BBO from scanner.") // Trace
					} else {
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
						}).Trace("BBO not found in scanner.") // Trace
					}

					// Get top N order book levels and metadata from Redis (via Scanner)
					// Scanner.GetOrderBookLevels now accepts context.
					bids, asks, lastUpdateID, lastUpdateTime, err := ws.scanner.GetOrderBookLevels(ctx, source, marketType, symbol, orderBookLimit) // *** PASS CTX AND MARKETTYPE ***
					if err != nil {
						// Error getting OB (not "not found" or context.Canceled) was logged in Scanner. Continue.
						logger.WithError(err).WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
						}).Debug("Error getting OrderBookLevels from scanner.") // Используем logrus logger Debug
					} else if len(bids) > 0 || len(asks) > 0 {
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
							"bids_count": len(bids), "asks_count": len(asks),
							"last_id": lastUpdateID, "last_time": lastUpdateTime,
						}).Trace("Fetched OrderBookLevels from scanner.") // Trace
					} else {
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
						}).Trace("OrderBookLevels not found in scanner.") // Trace
					}

					// Get sync status from Scanner's in-memory metadata
					syncStatus := "Initializing" // Default status if metadata not created by Scanner
					// Scanner.GetSyncMetadata now accepts MarketType.
					metadata, metaOk := ws.scanner.GetSyncMetadata(source, marketType, symbol) // *** PASS MARKETTYPE ***
					if metaOk {
						syncStatus = metadata.CurrentStatus // Use actual status from metadata
						// If data was found but metadata is missing, something is wrong.
						// If metadata exists but data wasn't found, that's expected (e.g., disconnected).
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
							"sync_status": syncStatus, "last_binance_id": metadata.LastUpdateID, "last_bybit_id": metadata.BybitUpdateID,
						}).Trace("Fetched SyncMetadata from scanner.") // Trace
					} else {
						// Log if metadata is unexpectedly missing for a tracked symbol/market type
						logger.WithFields(logrus.Fields{
							"source": source, "market_type": marketType, "symbol": symbol,
						}).Trace("Metadata not found in scanner.") // Trace
					}

					// --- Formulate data structure for this Source, MarketType, Symbol ---
					exchangeData := types.WebExchangeData{
						Source:         source,         // Explicitly set source
						MarketType:     marketType,     // Explicitly set market type
						Symbol:         symbol,         // Explicitly set symbol (uppercase)
						BBO:            bbo,            // Can be nil
						SyncStatus:     syncStatus,     // Actual sync status
						LastUpdateID:   lastUpdateID,   // Last OB update ID
						LastUpdateTime: lastUpdateTime, // Last OB update time
					}

					// Add order book data only if levels were successfully retrieved (at least one level slice is not empty).
					if len(bids) > 0 || len(asks) > 0 {
						exchangeData.OrderBook = &types.OrderBookWeb{
							Symbol: symbol, // Symbol (uppercase)
							Bids:   bids,   // Can be empty slice, but not nil
							Asks:   asks,   // Can be empty slice, but not nil
							// Timestamp and UpdateID are now in WebExchangeData
						}
					}

					// Add the data for this specific market type + symbol combination to the nested map.
					webData.Data[source][symbol][marketType] = exchangeData
				} // End symbol loop
			} // End sourceMarketKey loop

			// Marshal the collected data to JSON for sending to the client.
			jsonData, err := json.Marshal(webData)
			if err != nil {
				logger.WithError(err).Error("Error marshalling data for sending to client.") // Log JSON marshalling error
				// Do not consider this a fatal error, just skip sending in this cycle.
				continue
			}

			// Send JSON data to the client over WebSocket.
			writeTimeout := 5 * time.Second                            // Timeout for sending a message (TODO: From config)
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))        // Set deadline
			cErr := conn.WriteMessage(websocket.TextMessage, jsonData) // Send text message
			conn.SetWriteDeadline(time.Time{})                         // Reset deadline after sending

			if cErr != nil {
				// Write error on WebSocket connection. Usually means the connection is broken.
				// Log write errors at Warn level or higher, as they indicate a client disconnect or network issue.
				if websocket.IsCloseError(cErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					logger.Infof("WebSocket closed (write error): %v", cErr) // Log close error
				} else if _, ok := cErr.(net.Error); ok && strings.Contains(cErr.Error(), "use of closed network connection") {
					// "use of closed network connection" error means the connection was closed by another goroutine (e.g., reader or removeClient)
					logger.Debug("Write interrupted due to closed network connection.") // Log debug
				} else {
					logger.WithError(cErr).Warn("WebSocket write error.") // Log other write errors
				}
				// On any write error, assume connection is inactive and exit writer goroutine.
				return // Exit clientWriter goroutine. Defer ws.removeClient(conn) will be called.
			}

			// Data successfully sent. Select/for loop continues.
			logger.Trace("Data successfully sent to client.") // Trace

		case <-ctx.Done(): // Listen to context cancellation signal (main app context)
			logger.Info("Received context cancellation signal, exiting.") // Используем logrus logger Info
			// Optional: Send a close message to the client before returning
			// closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down")
			// conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(time.Second))
			return // Exit goroutine on context cancellation. Defer ws.removeClient(conn) will be called.
		}
	}
}

// StartServer launches the Gin HTTP server and sets up routes.
// This function is launched in a separate goroutine from main.go.
// ctx: Main application context for signalling termination.
// listenAddr: Address for the HTTP server to listen on.
// wsServer: WebSocketServer instance for handling WS connections.
// wg: WaitGroup from main.go for managing goroutine termination.
// logger: Logrus logger for StartServer function.
// *** ОБНОВЛЕННАЯ СИГНАТУРА для приема логгера ***
func StartServer(ctx context.Context, listenAddr string, wsServer *WebSocketServer, wg *sync.WaitGroup, logger *logrus.Entry) error { // >>> Принимаем logrus logger
	wg.Add(1)       // Add this goroutine (StartServer) to main WaitGroup
	defer wg.Done() // Notify main WaitGroup on exit

	// Используем переданный логгер
	logger.Infof("Web: Starting web server on %s...", listenAddr) // Используем logrus logger Infof

	// Gin setup
	// TODO: Configure Gin logging separately to avoid cluttering app logs under load
	// gin.SetMode(gin.ReleaseMode) // Use ReleaseMode in production
	// Gin's default logger writes to stdout. You can redirect it or disable it.
	// For now, we'll keep it simple. If it causes issues with logrus, configure Gin logging.
	r := gin.Default() // Default includes Logger and Recovery middleware

	// Route for WebSocket connections. HandleWebSocket handler launches reader/writer goroutines.
	// HandleWebSocket uses wsServer.serverCtx and wsServer.serverWg AND wsServer.log
	r.GET("/ws", wsServer.HandleWebSocket)

	// Route for static files (e.g., index.html for web UI)
	// TODO: Static file path from config
	logger.Debug("Serving static file from ./internal/web/static/index.html on /") // Логируем путь к статике
	r.StaticFile("/", "./internal/web/static/index.html")

	// Create HTTP server
	server := &http.Server{
		Addr:    listenAddr,
		Handler: r,
		// TODO: Add timeouts to config (ReadTimeout, WriteTimeout, IdleTimeout)
	}

	// Launch a goroutine that will block while the server is running (ListenAndServe).
	wg.Add(1) // Add this internal goroutine to WaitGroup
	go func() {
		defer wg.Done() // Notify WaitGroup when ListenAndServe finishes

		logger.Info("Web: Internal ListenAndServe goroutine launched.") // Используем logrus logger Info

		// ListenAndServe blocks until stopped (e.g., Shutdown or Close)
		// or a fatal error occurs (e.g., address already in use).
		// http.ErrServerClosed is the expected error when Shutdown or Close is called.
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log only fatal errors that are not graceful shutdown.
			logger.WithError(err).Fatal("Web: Failed to start web server.") // Используем logrus logger Fatal
			// In case of a fatal error (not ErrServerClosed), Fatal happens here,
			// which leads to os.Exit(1) and immediate application termination.
		}
		// This line is reached after a successful Shutdown or on http.ErrServerClosed.
		logger.Info("Web: Internal ListenAndServe goroutine finished.") // Используем logrus logger Info
	}()

	// Main logic of StartServer function: wait for shutdown signal from outside (from main).
	<-ctx.Done()                                                                                               // Blocks until external context (from main) is cancelled
	logger.Info("Web: Received context cancellation signal, initiating HTTP server and WebSocket shutdown...") // Используем logrus logger Info

	// Start graceful shutdown procedure for the HTTP server.
	// Use a new context for Shutdown with a timeout to prevent blocking shutdown too long.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second) // Shutdown timeout (TODO: From config)
	defer cancelShutdown()                                                                  // Ensure shutdown context cancellation

	if err := server.Shutdown(shutdownCtx); err != nil {
		// Log Shutdown error, but do not consider it fatal for the whole application.
		logger.WithError(err).Error("Web: HTTP Server Shutdown Error.") // Используем logrus logger Error
	} else {
		logger.Info("Web: HTTP server gracefully stopped.") // Используем logrus logger Info
	}

	// main.go's WaitGroup is now waiting for clientReader/clientWriter goroutines
	// because they call wg.Add(1) and defer wg.Done().
	// They listen to ctx.Done() (which is equal to ws.serverCtx, equal to ctx from main.go),
	// receive the signal, exit, call defer ws.removeClient(conn), and then defer wg.Done().
	logger.Info("Web: Shutdown signal sent to client WebSocket goroutines via context.") // Используем logrus logger Info
	// We don't wait for them explicitly here in StartServer, wg.Wait() in main.go does.

	logger.Info("Web: StartServer function finished.") // Используем logrus logger Info
	// defer wg.Done() at the beginning of the function ensures decrement in main.go.

	// StartServer returns nil on successful finish.
	return nil
}

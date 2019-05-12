package rabbitevent

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	defaultHeartbeat  = 10 * time.Second
	defaultLocale     = "en_US"
	defaultAmqpConfig = &amqp.Config{
		Heartbeat: defaultHeartbeat,
		Locale:    defaultLocale,
	}
	defaultConfig = &Config{
		Username:   "guest",
		Password:   "guest",
		Host:       "localhost",
		Port:       5672,
		Tls:        false,
		AmqpConfig: defaultAmqpConfig,
	}
	ErrNoConnectionConfig = errors.New("no config")
	ErrNotConnected       = errors.New("not connected to broker")
)

// Client amqp broker client
type Client struct {
	conn   *amqp.Connection
	config Config
	sync.Mutex
	closeBySelf                bool             // connection close by self
	connected                  bool             // is connected to amqp broker now
	logger                     logger           // logger
	logMode                    int              // logMode 1 for enable, 0 for disable
	notifyCloseChan            chan *amqp.Error // notify connection close channel
	onReconnectSuccessCallBack func()           // reconnect success callback function
	amqpChannel                *AmqpChannel     // default amqp channel
}

// Config config for amqp broker
type Config struct {
	Username    string
	Password    string
	VirtualHost string
	Host        string
	Port        int
	Tls         bool
	AmqpConfig  *amqp.Config
}

// GetConnString build amqp conn string by config
func (config *Config) GetConnString() string {
	tpl := "amqp://%s:%s@%s:%d/%s"
	if config.Tls {
		tpl = "amqps://%s:%s@%s:%d/%s"
	}
	return fmt.Sprintf(tpl,
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.VirtualHost)
}

// Init init rabbitmq broker with config
func Init(config *Config) {
	if config != nil {
		defaultConfig = config
	}
}

// NewClient new amqp broker client
func NewClient() (*Client, error) {
	if defaultConfig == nil {
		return nil, ErrNoConnectionConfig
	}
	client := &Client{
		config: *defaultConfig,
		logger: defaultLogger,
	}
	if err := client.Connect(); err != nil {
		return client, err
	}
	return client, nil
}

// Connect connect function to connect to amqp channel
func (client *Client) Connect() error {
	client.Lock()
	defer client.Unlock()

	if client.IsActive() {
		return nil
	}
	if client.config.AmqpConfig == nil {
		client.config.AmqpConfig = defaultAmqpConfig
	}
	conn, err := amqp.DialConfig(client.config.GetConnString(), *client.config.AmqpConfig)
	if err != nil {
		return err
	}

	client.conn = conn
	channel, err := client.Channel()
	if err != nil {
		return err
	}
	client.amqpChannel = channel
	client.connected = true
	notifyCloseChan := make(chan *amqp.Error)
	client.notifyCloseChan = notifyCloseChan
	conn.NotifyClose(notifyCloseChan)

	go client.handleClose()
	return nil
}

// handleClose handle connection close event
func (client *Client) handleClose() {
	for {
		select {
		case <-client.notifyCloseChan:
			client.log("recv connection close")
			if client.closeBySelf {
				client.log("connection close by self")
				return
			}
			client.connected = false
			for {
				client.log("try to reconnect to broker")
				if err := client.Connect(); err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				if client.onReconnectSuccessCallBack != nil {
					client.onReconnectSuccessCallBack()
				}
				return
			}
		}
	}
}

// onReconnectSuccess on reconnect success event callback setter
func (client *Client) onReconnectSuccess(callback func()) {
	client.onReconnectSuccessCallBack = callback
}

// Channel return the wrapper of amqp channel
func (client *Client) Channel() (amqpChannel *AmqpChannel, err error) {
	amqpChannel = new(AmqpChannel)
	if client.conn == nil {
		return amqpChannel, ErrNotConnected
	}

	channel, err := client.conn.Channel()
	if err != nil {
		return amqpChannel, ErrNotConnected
	}

	amqpChannel.channel = channel
	amqpChannel.conn = client.conn
	return amqpChannel, nil
}

// IsActive check for connection is active
func (client *Client) IsActive() bool {
	if client.conn == nil {
		return false
	}
	channel, err := client.conn.Channel()
	if err != nil {
		return false
	}
	_ = channel.Close()
	return true
}

// Close function for close client conn
func (client *Client) Close() {
	client.Lock()
	defer client.Unlock()
	if client.conn != nil && !client.closeBySelf {
		client.closeBySelf = true
		client.connected = false
		close(client.notifyCloseChan)
	}
}

// QueueConfig config for queue
type QueueConfig struct {
	Name      string
	Durable   bool
	WorkerNum int
	Args      amqp.Table
}

// DeclareQueue proxy for amqpChannel DeclareQueue function
func (client *Client) DeclareQueue(config QueueConfig) (amqp.Queue, error) {
	return client.amqpChannel.DeclareQueue(config)
}

// ExchangeConfig config for exchange
type ExchangeConfig struct {
	Name    string
	Kind    string
	Durable bool
	Args    amqp.Table
}

// DeclareExchange proxy amqpChannel declareExchange function
func (client *Client) DeclareExchange(config ExchangeConfig) error {
	return client.amqpChannel.DeclareExchange(config)
}

// QueueBind proxy amqpChannel QueueBind function
func (client *Client) QueueBind(queueName, exchangeName, bindingKey string) error {
	return client.amqpChannel.QueueBind(queueName, exchangeName, bindingKey)
}

// LogMode set log mode for client
func (client *Client) LogMode(enable bool) *Client {
	if enable {
		client.logMode = logModeEnable
	} else {
		client.logMode = logModeDisable
	}
	return client
}

func (client *Client) log(v ...interface{}) {
	if client.logMode == logModeEnable && client.logger != nil {
		client.logger.Print("time:", time.Now().Format("2006/01/02-15:04:05"), " | ", v)
	}
}

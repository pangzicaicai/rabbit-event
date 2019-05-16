package rabbitevent

import (
	"github.com/streadway/amqp"
	"time"
)

const (
	defaultPublisherWorkerNum = 3
)

// Publisher base on amqp exchange
type Publisher struct {
	*Client
	config            *PublisherConfig
	messageQueue      chan Message
	workerRestartChan chan int
	messageStore      MessageStore
	onReconnect       chan int
}

// PublisherConfig config for publisher
type PublisherConfig struct {
	Exchange     *ExchangeConfig
	WorkerNum    int
	MessageStore MessageStore
}

// NewPublisher new publisher with given config
func NewPublisher(config *PublisherConfig) (publisher *Publisher, err error) {
	publisher = new(Publisher)
	client, err := NewClient()
	if err != nil {
		return publisher, err
	}
	publisher.Client = client

	if config.MessageStore == nil {
		config.MessageStore = newDefaultMessageStore()
	}
	publisher.config = config
	publisher.messageStore = config.MessageStore
	publisher.messageQueue = make(chan Message, 100)
	publisher.onReconnect = make(chan int)
	publisher.onReconnectSuccess(publisher.signalReconnect)
	return publisher, err
}

func NewPublisherConfig(exchange *ExchangeConfig) *PublisherConfig {
	return &PublisherConfig{
		Exchange:     exchange,
		WorkerNum:    defaultPublisherWorkerNum,
		MessageStore: newDefaultMessageStore(),
	}
}

// ExchangeName shortcut for get subscriber config exchange name
func (p *Publisher) ExchangeName() string {
	if p.config == nil || p.config.Exchange == nil {
		return ""
	}
	return p.config.Exchange.Name
}

func (p *Publisher) signalReconnect() {
	p.onReconnect <- 1
}

// Work start the publisher
func (p *Publisher) Work() (err error) {
	if p.config == nil {
		return ErrNoPublisherConfig
	}

	if p.config.Exchange == nil {
		return ErrNoExchangeConfig
	}

	if err = validateExchangeConfig(*p.config.Exchange); err != nil {
		return err
	}

	if err = p.DeclareExchange(*p.config.Exchange); err != nil {
		return err
	}

	workerNum := p.config.WorkerNum
	if workerNum == 0 {
		workerNum = defaultPublisherWorkerNum
	}

	go p.handleWorkSignature()

	for i := 0; i < workerNum; i++ {
		go p.startWorker(i)
	}
	return nil
}

// PublishDirectly publish message to broker directly
func (p *Publisher) PublishDirectly(message Message) (err error) {
	// 直接发, 发完直接拿到结果
	channel, err := p.Channel()
	if err != nil {
		return err
	}

	return channel.Publish(p.ExchangeName(), &message)
}

func (p *Publisher) Publish(message Message) {
	// 如果直接发, 那么需要拿到一个channel
	// 如果维护一个client里面的channel, 那么如果同一时刻有多个事件要发送, 那么就会出现阻塞
	// 如果发送失败, 或者连接断开, 那么 业务方可能无法很好处理
	p.messageQueue <- message
}

func (p *Publisher) startWorker(workerNo int) {
	ch, err := p.Channel()
	if err != nil {
		p.workerRestartChan <- workerNo
		return
	}
	defer ch.Close()
	for {
		select {
		case msg := <-p.messageQueue:
			p.log("[publisher] receive msg, exchange:", p.ExchangeName(),
				", event:", msg.Event)
			if err = ch.Publish(p.ExchangeName(), &msg); err != nil {
				p.log("[publisher] publish msg error, exchange:", p.ExchangeName(),
					", event:", msg.Event, "err:", err)
				if err == amqp.ErrClosed {
					p.messageStore.Push(msg)
					p.workerRestartChan <- workerNo
					return
				}
			}
		}
	}
}

func (p *Publisher) handleWorkSignature() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case workerNo := <-p.workerRestartChan:
			go p.startWorker(workerNo)
		case <-p.onReconnect:
			go p.handleMessageStore()
		case <-t.C:
			go p.handleMessageStore()
		}
	}
}

func (p *Publisher) handleMessageStore() {
	messages := p.messageStore.Pull()
	if messages != nil && len(messages) > 0 {
		for _, msg := range messages {
			p.Publish(msg)
		}
	}
}

package rabbitevent

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"regexp"
	"strings"
)

var (
	ErrNoQueueSubscriber  = errors.New("no queue subscriber")
	ErrNoSubscriberConfig = errors.New("no subscriber config")
)

const (
	defaultSubscriberWorker = 3
	defaultMaxRetries       = 3
)

// Subscriber amqp queue subscriber
type Subscriber struct {
	*Client
	config                *SubscriberConfig
	queueSubscriberMapper map[string]QueueSubscriber
	compileTopicMapper    map[string]map[string]*regexp.Regexp // map[queue]map[topic]*regexp.Regexp
	workerRestartChan     chan subscriberWorker
}

// SubscriberHandleFunc alias for handler func for subscriber
type SubscriberHandleFunc func(Message) (shouldRetry bool)

// SubscriberHandleFunc list for handler func for subscriber
type SubscriberHandleFuncs []SubscriberHandleFunc

// EventHandleFuncsMapper map for topic and its handle func
type EventHandleFuncsMapper map[string]SubscriberHandleFuncs

// QueueSubscriber define queue subscriber handle funcs
type QueueSubscriber struct {
	Queue                  QueueConfig
	EventHandleFuncsMapper EventHandleFuncsMapper
}

// QueueSubscribers list for QueueSubscriber
type QueueSubscribers []QueueSubscriber

type subscriberWorker struct {
	Queue string
	No    int
}

func newSubscriberWorker(queue string, workerNo int) subscriberWorker {
	return subscriberWorker{
		Queue: queue,
		No:    workerNo,
	}
}

// SubscriberConfig config for subscriber
type SubscriberConfig struct {
	Exchange                         *ExchangeConfig // Exchange config
	RepushToQueueWhenNoHandler       bool
	AckWhenNoHandler                 bool
	AutoAck                          bool
	RepushToQueueWhenHandleFuncPanic bool
	AckWhenHandleFuncPanic           bool
	DefaultWorkerNum                 int
	MaxTries                         int
	SubscriberHandleFuncPanicDealer  func(message Message)
	OnConsumeFail                    func()
	PrefetchCount                    int
	PrefetchGlobal                   bool
}

// NewSubscriberConfig make config for subscriber with default options
func NewSubscriberConfig(exchange *ExchangeConfig) *SubscriberConfig {
	return &SubscriberConfig{
		Exchange:                         exchange,
		RepushToQueueWhenNoHandler:       true,
		AckWhenNoHandler:                 false,
		AutoAck:                          false,
		RepushToQueueWhenHandleFuncPanic: true,
		AckWhenHandleFuncPanic:           false,
		DefaultWorkerNum:                 defaultSubscriberWorker,
		MaxTries:                         defaultMaxRetries,
		SubscriberHandleFuncPanicDealer:  nil,
		PrefetchCount:                    0,
		PrefetchGlobal:                   false,
	}
}

func NewSubscriber(cfg *SubscriberConfig) (subscriber *Subscriber, err error) {
	client, err := NewClient()
	if err != nil {
		return nil, err
	}

	subscriber = &Subscriber{
		Client:                client,
		config:                cfg,
		queueSubscriberMapper: make(map[string]QueueSubscriber),
		compileTopicMapper:    make(map[string]map[string]*regexp.Regexp),
		workerRestartChan:     make(chan subscriberWorker),
	}

	return subscriber, err
}

// RegisterQueueSubscribers register list for subscriber
func (s *Subscriber) RegisterQueueSubscribers(subscribers QueueSubscribers) {
	s.RegisterQueueSubscriber(subscribers...)
}

// RegisterQueueSubscriber register multi single subscriber
func (s *Subscriber) RegisterQueueSubscriber(subscribers ...QueueSubscriber) {
	for _, subscriber := range subscribers {
		s.registerQueueSubscriber(subscriber)
	}
}

// registerQueueSubscriber register single subscriber
func (s *Subscriber) registerQueueSubscriber(subscriber QueueSubscriber) {
	queueName := subscriber.Queue.Name
	queueSubscriber, exists := s.queueSubscriberMapper[queueName]
	if !exists {
		s.queueSubscriberMapper[queueName] = subscriber
		return
	} else {
		for topic, handlerFuncs := range subscriber.EventHandleFuncsMapper {
			topicHandleFuncs, exists := queueSubscriber.EventHandleFuncsMapper[topic]
			if !exists {
				topicHandleFuncs = make(SubscriberHandleFuncs, 0)
			}
			// merge multi handle func when topic is exists
			topicHandleFuncs = append(topicHandleFuncs, handlerFuncs...)
			queueSubscriber.EventHandleFuncsMapper[topic] = topicHandleFuncs
		}
		s.queueSubscriberMapper[queueName] = queueSubscriber
	}
}

// ExchangeName shortcut for get subscriber config exchange name
func (s *Subscriber) ExchangeName() string {
	if s.config == nil || s.config.Exchange == nil {
		return ""
	}
	return s.config.Exchange.Name
}

// Work start the subscriber
func (s *Subscriber) Work() (err error) {
	if s.config == nil {
		return ErrNoSubscriberConfig
	}
	if s.config.Exchange == nil {
		return ErrNoExchangeConfig
	}

	if err = validateExchangeConfig(*s.config.Exchange); err != nil {
		return err
	}

	if s.queueSubscriberMapper == nil || len(s.queueSubscriberMapper) == 0 {
		return ErrNoQueueSubscriber
	}

	if err = s.DeclareExchange(*s.config.Exchange); err != nil {
		return err
	}

	go s.handleWorkerSignature()

	// 声明queue
	for queueName, queueSubscriber := range s.queueSubscriberMapper {
		if len(queueSubscriber.EventHandleFuncsMapper) == 0 {
			return fmt.Errorf("no subscribe topic for queue:%s", queueName)
		}
		if err = validateQueueConfig(queueSubscriber.Queue); err != nil {
			return err
		}
		queue, err := s.DeclareQueue(queueSubscriber.Queue)
		if err != nil {
			return err
		}
		for topic, _ := range queueSubscriber.EventHandleFuncsMapper {
			if len(queueSubscriber.EventHandleFuncsMapper) == 0 {
				return fmt.Errorf("no handle func for queue:%s, topic:%s", queueName, topic)
			}
			if err = s.QueueBind(queue.Name, s.ExchangeName(), topic); err != nil {
				return err
			}

			d, exists := s.compileTopicMapper[queueName]
			if !exists {
				d = make(map[string]*regexp.Regexp)
			}
			// compile topic rule
			d[topic] = compileTopic(topic)
			s.compileTopicMapper[queueName] = d
		}

		workerNum := s.config.DefaultWorkerNum
		if queueSubscriber.Queue.WorkerNum > 0 {
			workerNum = queueSubscriber.Queue.WorkerNum
		}

		for i := 0; i < workerNum; i++ {
			go s.startWorker(queueName, i)
		}
	}

	return nil
}

func compileTopic(topic string) *regexp.Regexp {
	compileTopic := strings.Replace(topic, ".", `\.`, -1)
	compileTopic = strings.Replace(compileTopic, "*", ".{1}", -1)
	compileTopic = strings.Replace(compileTopic, "#", ".*", -1)

	return regexp.MustCompile(compileTopic)
}

func (s *Subscriber) startWorker(queue string, workerNo int) {
	worker := newSubscriberWorker(queue, workerNo)
	ch, err := s.Channel()
	if err != nil {
		s.workerRestartChan <- worker
		return
	}
	defer ch.Close()
	if err = ch.channel.Qos(s.config.PrefetchCount, 0, s.config.PrefetchGlobal); err != nil {
		s.workerRestartChan <- worker
		return
	}

	queueSubscriber, exists := s.queueSubscriberMapper[queue]
	if !exists {
		return
	}

	compileTopicMapper, exists := s.compileTopicMapper[queue]
	if !exists {
		return
	}

	amqpMsgChan, err := ch.channel.Consume(
		queue,
		genConsumerName(workerNo),
		s.config.AutoAck,
		false,
		false,
		false,
		nil)
	if err != nil {
		s.workerRestartChan <- worker
		return
	}

	for amqpMsg := range amqpMsgChan {
		s.log("[subscriber] receive msg, exchange:", s.ExchangeName(), ", queue:", queue,
			", routing key:", amqpMsg.RoutingKey)
		topicKey := matchTopicKey(compileTopicMapper, amqpMsg.RoutingKey)
		if len(topicKey) == 0 {
			s.log("routing key:", amqpMsg.RoutingKey, " match no handle funcs")
			if s.config.RepushToQueueWhenNoHandler {
				amqpMsg.Reject(true)
				continue
			}
			if s.config.AckWhenNoHandler {
				amqpMsg.Ack(false)
				continue
			}
			continue
		}

		handleFuncs := queueSubscriber.EventHandleFuncsMapper[topicKey]

		go s.amqpMsgHandleFunc(queue, amqpMsg, handleFuncs)
	}

	s.workerRestartChan <- worker
}

func (s *Subscriber) amqpMsgHandleFunc(queue string, amqpMsg amqp.Delivery, handleFuncs SubscriberHandleFuncs) {
	message := Message{
		Event:       amqpMsg.RoutingKey,
		Headers:     amqpMsg.Headers,
		Body:        amqpMsg.Body,
		ContentType: amqpMsg.ContentType,
	}

	maxTries := s.config.MaxTries

	defer func() {
		if e := recover(); e != nil {
			s.log("[subscriber] panic recover, exchange:", s.ExchangeName(), ", queue:", queue,
				", routing key:", amqpMsg.RoutingKey)
			if s.config.SubscriberHandleFuncPanicDealer != nil {
				s.config.SubscriberHandleFuncPanicDealer(message)
			}
			if s.config.RepushToQueueWhenHandleFuncPanic {
				amqpMsg.Reject(true)
			}
		}
	}()

	for _, handleFunc := range handleFuncs {
		for retryTimes := 0; retryTimes < maxTries; retryTimes++ {
			shouldRetry := handleFunc(message)
			if !shouldRetry {
				break
			}
		}
	}
	s.log("[subscriber] finish handle msg, exchange:", s.ExchangeName(), ", queue:", queue,
		", routing key:", amqpMsg.RoutingKey)
	amqpMsg.Ack(false)
}

// matchTopicKey match routing key and topic key
func matchTopicKey(compileTopicMap map[string]*regexp.Regexp, routingKey string) string {
	if _, exists := compileTopicMap[routingKey]; exists {
		return routingKey
	}

	for event, pattern := range compileTopicMap {
		if !strings.Contains(event, "*") && !strings.Contains(event, "#") {
			return ""
		}
		if pattern.FindString(routingKey) == routingKey {
			return event
		}
	}

	return ""
}

func genConsumerName(workerNum int) string {
	return fmt.Sprintf("%s-%d", localIp(), workerNum)
}

func (s *Subscriber) handleWorkerSignature() {
	for {
		select {
		case worker := <-s.workerRestartChan:
			go s.startWorker(worker.Queue, worker.No)
		}
	}
}

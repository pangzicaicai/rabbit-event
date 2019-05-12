package rabbitevent

import "github.com/streadway/amqp"

const (
	contentJson = "application/json"
)

// AmqpChannel wrapper of amqp channel
type AmqpChannel struct {
	channel *amqp.Channel
	conn    *amqp.Connection
}

// Close close current amqp channel
func (ch *AmqpChannel) Close() {
	if ch == nil || ch.channel == nil {
		return
	}
	_ = ch.channel.Close()
}

// Publish publish msg using amqp channel
func (ch *AmqpChannel) Publish(exchange string, msg *Message) error {
	headers := make(amqp.Table)
	if msg.Headers != nil && len(msg.Headers) > 0 {
		headers = msg.Headers
	}

	contentType := msg.ContentType
	if len(contentType) == 0 {
		contentType = contentJson
	}
	return ch.channel.Publish(
		exchange,
		msg.Event,
		false,
		false,
		amqp.Publishing{
			Headers:     headers,
			Body:        msg.Body,
			ContentType: contentType,
		},
	)
}

// QueueBind bind queue and exchange using binding key
func (ch *AmqpChannel) QueueBind(queueName, exchangeName, bindingKey string) error {
	return ch.channel.QueueBind(
		queueName,
		bindingKey,
		exchangeName,
		false,
		nil)
}

// DeclareExchange declare exchange by given exchange config
func (ch *AmqpChannel) DeclareExchange(config ExchangeConfig) error {
	return ch.channel.ExchangeDeclare(
		config.Name,
		config.Kind,
		config.Durable,
		false,
		false,
		false,
		config.Args)
}

// DeclareQueue declare queue by given queue config
func (ch *AmqpChannel) DeclareQueue(config QueueConfig) (amqp.Queue, error) {
	return ch.channel.QueueDeclare(
		config.Name,
		config.Durable,
		false,
		false,
		false,
		config.Args)
}

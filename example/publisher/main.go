package main

import (
	"encoding/json"
	"github.com/pangzicaicai/rabbitevent"
	"github.com/streadway/amqp"
	"log"
)

func main() {
	// inject rabbitmq basic config
	config := rabbitevent.Config{
		Username: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	}
	rabbitevent.Init(&config)

	// make publisher config with default options
	publisherCfg := rabbitevent.NewPublisherConfig(
		&rabbitevent.ExchangeConfig{
			Name:    "rabbit-event",
			Kind:    amqp.ExchangeTopic,
			Durable: true,
		})

	// new publisher with config
	publisher, err := rabbitevent.NewPublisher(publisherCfg)

	if err != nil {
		log.Fatal(err)
	}
	publisher.LogMode(true)
	// start the publisher
	if err = publisher.Work(); err != nil {
		log.Fatal(err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"foo": "bar",
	})

	if err != nil {
		log.Fatal(err)
	}

	// publish msg
	publisher.Publish(rabbitevent.Message{
		Event: "event1",
		Body:  data,
	})

	// block the main goroutine
	select {}
}

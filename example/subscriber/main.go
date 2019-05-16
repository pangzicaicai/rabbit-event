package main

import (
	"fmt"
	"github.com/pangzicaicai/rabbitevent"
	"github.com/streadway/amqp"
	"log"
	"time"
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

	// make subscriber config with default options
	subscriberCfg := rabbitevent.NewSubscriberConfig(&rabbitevent.ExchangeConfig{
		Name:    "rabbit-event",
		Kind:    amqp.ExchangeTopic,
		Durable: true,
	})

	subscriber, err := rabbitevent.NewSubscriber(subscriberCfg)
	if err != nil {
		log.Fatal(err)
	}

	// register queue subscriber
	subscriber.RegisterQueueSubscribers(QueueSubscribers)
	//subscriber.RegisterQueueSubscribers(QueueSubscriberV2)

	// start the subscriber
	if err = subscriber.Work(); err != nil {
		log.Fatal(err)
	}

	// block the main goroutine
	select {}
}

var QueueSubscribers = rabbitevent.QueueSubscribers{
	{
		Queue: rabbitevent.QueueConfig{
			Name:    "rabbit-event-not-durable",
			Durable: false,
		},
		EventHandleFuncsMapper: rabbitevent.EventHandleFuncsMapper{
			"event1": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event, event:event1, handle_func 1 receive msg")
					fmt.Println(string(message.Body))
					return false
				},
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event, event:event1, handle_func2 receive msg")
					fmt.Println(string(message.Body))
					return false
				},
			},
			"event2": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event, event:event2, handle_func 1 receive msg")
					return false
				},
			},
		},
	},
	{
		Queue: rabbitevent.QueueConfig{
			Name:      "rabbit-event-v2",
			Durable:   true,
			WorkerNum: 1,
		},
		EventHandleFuncsMapper: rabbitevent.EventHandleFuncsMapper{
			"#": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event-v2, event:#, handle_func receive msg")
					fmt.Println(string(message.Body))
					return false
				},
			},
		},
	},
}

var QueueSubscriberV2 = rabbitevent.QueueSubscribers{
	{
		Queue: rabbitevent.QueueConfig{
			Name:    "rabbit-event",
			Durable: true,
		},
		EventHandleFuncsMapper: rabbitevent.EventHandleFuncsMapper{
			"event3": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event, event:event3, handle_func receive msg")

					return false
				},
			},
			"event2": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					Log("queue: rabbit-event, event:event2, handle_func3 receive msg")
					fmt.Println(message)
					return true
				},
			},
		},
	},
}

func Log(v ...interface{}) {
	fmt.Println("time:", time.Now().Format("2006/01/02-15:04:05"), " | ", v)
}

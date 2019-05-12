# rabbit-event (rabbit-event)
Simple wrapper for RabbitMQ(amqp) client. Base on pub/sub model.

## Quick Start
+ 在一个publisher/subscriber对象中, 只关注一个`exchange`内的事件通信,  
+ publisher 只往exchange中发送消息, 指定相应的主题。
+ subscriber 关注感兴趣的 主题。可以自定义queue, 并在queue中消费相应监听的事情
+ 一个 队列 可以使用 bindingKey 绑定多个事件, 但是同一个事件在 在 队列中一次只会发送一个 
+ 对于绑定了多个事件的队列, 应用层内部会做分发, 分发给最先匹配到的关注者进行处理


### Subscriber
```go
package main

import (
	"github.com/streadway/amqp"
	"github.com/pangzicaicai/rabbitevent"
	"log"
)

var queueSubscribers = rabbitevent.QueueSubscribers{
	{
		Queue: rabbitevent.QueueConfig{
			Name:      "rabbit-event",
			Durable:   true,
		},
		EventHandleFuncsMapper: rabbitevent.EventHandleFuncsMapper{
			"event1": rabbitevent.SubscriberHandleFuncs{
				func(message rabbitevent.Message) bool {
					log.Println( "queue: rabbit-event, event:event1, handle_func receive msg")
					log.Println(string(message.Body))
					return false
				},
			},
		},
	},
}

func main() {
        // 注入rabbitmq启动参数
     	config := rabbitevent.Config{
     		Username: "guest",
     		Password: "guest",
     		Host:     "localhost",
     		Port:     5672,
     	}
     	rabbitevent.Init(&config)
     
     	// 创建publisher者配置, 带默认值
     	subscriberCfg := rabbitevent.NewSubscriberConfig(&rabbitevent.ExchangeConfig{
     		Name:    "rabbit-event",
     		Kind:    amqp.ExchangeTopic,
     		Durable: true,
     	})
        
     	// 获取 subscriber 对象, 连接到rabbitmq
     	subscriber, err := rabbitevent.NewSubscriber(subscriberCfg)
     	if err != nil {
     		log.Fatal(err)
     	}
        
     	// 注册相应的队列关注者
     	subscriber.RegisterQueueSubscribers(queueSubscribers)
        
     	// 启动发布者
     	if err = subscriber.Work(); err != nil {
     		log.Fatal(err)
     	}
        
     	// block the main goroutine
     	select {}
}
```

### Publisher
```go
package main

import (
    "log"
    "github.com/streadway/amqp"
    "github.com/pangzicaicai/rabbitevent"
)

func main () {
	// 注入rabbitmq启动配置
	config := rabbitevent.Config{
    		Username: "guest",
    		Password: "guest",
    		Host:     "localhost",
    		Port:     5672,
    	}
    	rabbitevent.Init(&config)
    
    	// 创建publisher配置,带默认值, 连接到mq
    	publisherCfg := rabbitevent.NewPublisherConfig(&rabbitevent.ExchangeConfig{
    		Name:    "rabbit-event",
    		Kind:    amqp.ExchangeTopic,
    		Durable: true,
    	})
    
    	// 获取publisher对象
    	publisher, err := rabbitevent.NewPublisher(publisherCfg)
    	if err != nil {
    		log.Fatal(err)
    	}
    
    	// 启动publisher, 可以开始发送消息
    	if err = publisher.Work(); err != nil {
    		log.Fatal(err)
    	}
    	
    	select{}
}
```

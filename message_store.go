package rabbitevent

import (
	"sync"

	"github.com/streadway/amqp"
)

// MessageStore temp message store when connection fail
type MessageStore interface {
	// Push push message to message store
	Push(message Message)
	// Pull message list from message store
	Pull() []Message
}

// Message message to publish or subscriber
type Message struct {
	Event       string
	Headers     amqp.Table
	Body        []byte
	ContentType string
}

type FailJob struct {
	Exchange  string
	Queue     string
	Message   Message
	Timestamp int64
}

type defaultMessageStore struct {
	sync.Mutex
	messages []Message
}

// Pull pull message from default message store
func (store *defaultMessageStore) Pull() []Message {
	store.Lock()
	defer store.Unlock()
	messages := store.messages
	store.messages = make([]Message, 0)
	return messages
}

// Push push message to default message store
func (store *defaultMessageStore) Push(message Message) {
	store.Lock()
	defer store.Unlock()
	store.messages = append(store.messages, message)
}

func newDefaultMessageStore() MessageStore {
	return &defaultMessageStore{
		messages: make([]Message, 0),
	}
}

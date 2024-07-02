package executer

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

// sync.Map is too generic so needs direct locking

type Engine interface {
	Execute(topic string, request Request) error
	ExecuteWith(topic string, request Request, timeout time.Duration) error
	AddConsumer(topic string, listener Consumer)
	AddConsumerBuffered(topic string, listener Consumer, requestbuffersize int)
	RemoveConsumer(topic string, consunerid uuid.UUID) bool
	HasConsumers(topic string) bool
	GetConsumers(topic string) []Consumer
	Start() error
	Stop() error
}

type Consumer struct {
	id      uuid.UUID
	Exit    chan bool
	handler func(request Request)
}

func NewConsumer(handler func(request Request)) *Consumer {
	nc := new(Consumer)
	nc.id = uuid.New()
	nc.Exit = make(chan bool, 1)
	nc.handler = handler
	return nc
}

func (c Consumer) GetID() uuid.UUID {
	return c.id
}

func (c *Consumer) Set(handler func(request Request)) {
	c.handler = handler
}

type engine struct {
	topicRegistry map[string]chan Request
	topiclocker   *sync.RWMutex

	consumerregistry sync.Map
}

func (e *engine) Execute(topic string, request Request) error {
	e.topiclocker.Lock()
	rchan, ok := e.topicRegistry[topic]
	e.topiclocker.Unlock()
	if ok {
		rchan <- request
		return nil
	}
	err := errors.New("No topic with name " + topic)
	log.Printf("Unlocking and error %+v", err)

	return err

}
func (e *engine) ExecuteWith(topic string, request Request, timeout time.Duration) error {
	e.topiclocker.Lock()
	rchan, ok := e.topicRegistry[topic]
	e.topiclocker.Unlock()
	if ok {
		select {
		case rchan <- request:
			return nil
		case <-time.After(timeout):
			return errors.New("send timed out")
		}
	}
	err := errors.New("No topic with name " + topic)
	log.Printf("Unlocking and error %+v", err)

	return err

}
func (e *engine) AddConsumer(topic string, listener Consumer) {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()

	if _, ok := e.topicRegistry[topic]; !ok {
		e.topicRegistry[topic] = make(chan Request, requestBufferSize)
	}
	e.setuplistener(listener, e.topicRegistry[topic])
	consumermap, ok := e.consumerregistry.Load(topic)
	var amap map[uuid.UUID]Consumer
	if ok {
		amap = consumermap.(map[uuid.UUID]Consumer)
	} else {
		amap = make(map[uuid.UUID]Consumer)
	}
	amap[listener.GetID()] = listener
	e.consumerregistry.Store(topic, amap)
}

func (e *engine) AddConsumerBuffered(topic string, listener Consumer, requestbuffersize int) {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()

	if _, ok := e.topicRegistry[topic]; !ok {
		e.topicRegistry[topic] = make(chan Request, requestbuffersize)
	}
	e.setuplistener(listener, e.topicRegistry[topic])
	consumermap, ok := e.consumerregistry.Load(topic)
	var amap map[uuid.UUID]Consumer
	if ok {
		amap = consumermap.(map[uuid.UUID]Consumer)
	} else {
		amap = make(map[uuid.UUID]Consumer)
	}
	amap[listener.GetID()] = listener
	e.consumerregistry.Store(topic, amap)
}

func (e *engine) RemoveConsumer(topic string, consunerid uuid.UUID) bool {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()

	rc, ok := e.topicRegistry[topic]
	if !ok {
		return false
	}

	consumermap, ok := e.consumerregistry.Load(topic)
	var amap map[uuid.UUID]Consumer
	if ok {
		amap = consumermap.(map[uuid.UUID]Consumer)
		cn, ok := amap[consunerid]
		if ok {
			delete(amap, consunerid)
			if len(amap) == 0 {
				e.consumerregistry.Delete(topic)
				delete(e.topicRegistry, topic)
				select {
				case cn.Exit <- true:
				case <-time.After(time.Millisecond * 100):
				}
				close(rc)
			} else {
				e.consumerregistry.Store(topic, amap)
			}
			return true
		}
	}
	return false

}

func (e *engine) HasConsumers(topic string) bool {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()

	_, ok := e.topicRegistry[topic]

	return ok

}
func (e *engine) GetConsumers(topic string) []Consumer {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()
	consumermap, ok := e.consumerregistry.Load(topic)
	if ok {
		amap := consumermap.(map[uuid.UUID]Consumer)
		cns := make([]Consumer, 0)
		if amap != nil {
			for _, c := range amap {
				cns = append(cns, c)
			}
		}
		return cns
	} else {
		return make([]Consumer, 0)
	}
}

func (e *engine) setuplistener(listener Consumer, channel <-chan Request) {
	go func() {
		for {
			select {
			case <-listener.Exit:
				return
			case req, ok := <-channel:
				if ok {
					listener.handler(req)
				} else {
					log.Println("Closed")
					return
				}
			}
		}
	}()
}

func (e *engine) Start() error {
	return nil
}
func (e *engine) Stop() error {
	e.topiclocker.Lock()
	defer e.topiclocker.Unlock()
	e.consumerregistry.Range(func(key any, value any) bool {
		topic := key.(string)
		var amap map[uuid.UUID]Consumer
		amap = value.(map[uuid.UUID]Consumer)
		for id, con := range amap {
			select {
			case con.Exit <- true:
			case <-time.After(time.Millisecond * 200):
			}
			delete(amap, id)
		}
		e.consumerregistry.Delete(topic)
		return true
	})

	for t, rchan := range e.topicRegistry {
		close(rchan)
		delete(e.topicRegistry, t)
	}

	return nil
}

func NewEngine() Engine {
	en := new(engine)
	en.topicRegistry = make(map[string]chan Request)
	en.topiclocker = new(sync.RWMutex)
	return en
}

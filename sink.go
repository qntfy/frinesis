package frinesis

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/qntfy/frinesis/batchproducer"
	"github.com/qntfy/frizzle"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	_ frizzle.Sink    = (*Sink)(nil)
	_ frizzle.Eventer = (*Sink)(nil)

	defaultFlushTimeout = 30 * time.Second
)

// Sink provides a frizzle interface for writing to AWS Kinesis
type Sink struct {
	client       *kinesis.Kinesis
	config       *batchproducer.Config
	prods        map[string]batchproducer.Producer
	prodMux      sync.RWMutex
	flushTimeout time.Duration
	evtChan      chan frizzle.Event
}

// InitSink initializes a basic sink with no logging
func InitSink(config *viper.Viper) (*Sink, error) {
	return InitSinkWithLogger(config, zap.NewNop())
}

// InitSinkWithLogger initializes a basic Sink with a provided logger
func InitSinkWithLogger(config *viper.Viper, logger *zap.Logger) (*Sink, error) {
	client, err := ClientFromViper(config)
	if err != nil {
		return nil, err
	}

	flushTimeout := defaultFlushTimeout
	if config.IsSet("kinesis_flush_timeout") {
		flushTimeout = config.GetDuration("kinesis_flush_timeout")
	}

	// TODO: further optimize the config
	cfg := batchproducer.DefaultConfig
	cfg.BatchSize = 500
	cfg.Logger = logger

	s := &Sink{
		client:       client,
		config:       &cfg,
		prods:        make(map[string]batchproducer.Producer),
		flushTimeout: flushTimeout,
		evtChan:      make(chan frizzle.Event),
	}
	return s, nil
}

// Send a Msg to topic. Initializes a kinesis producer for this topic if this
// Sink hasn't sent to it before.
func (s *Sink) Send(m frizzle.Msg, topic string) error {
	s.prodMux.RLock()
	prod, ok := s.prods[topic]
	s.prodMux.RUnlock()
	if !ok {
		var err error
		if prod, err = s.addTopicProducer(topic); err != nil {
			return err
		}
	}
	return prod.Add(m.Data(), generateID())
}

func (s *Sink) addTopicProducer(topic string) (batchproducer.Producer, error) {
	s.prodMux.Lock()
	defer s.prodMux.Unlock()
	prod, ok := s.prods[topic]
	if ok {
		// topic producer already exists
		return prod, nil
	}
	prod, err := batchproducer.New(s.client, topic, *s.config)
	if err != nil {
		return nil, err
	}
	if err = prod.Start(); err != nil && err != batchproducer.ErrAlreadyStarted {
		return nil, err
	}

	// Pass along async events from batchproducer
	go func() {
		for e := range prod.Events() {
			s.evtChan <- frizzle.Event(e)
		}
	}()

	s.prods["topic"] = prod
	return prod, nil
}

// Events reports async Events that occur during processing
func (s *Sink) Events() <-chan frizzle.Event {
	return (<-chan frizzle.Event)(s.evtChan)
}

// Close the Sink
func (s *Sink) Close() error {
	s.prodMux.Lock()
	defer s.prodMux.Unlock()

	for topic := range s.prods {
		_, remaining, err := s.prods[topic].Flush(s.flushTimeout, true)
		if err != nil && err != batchproducer.ErrAlreadyStopped {
			return err
		}
		if remaining > 0 {
			return fmt.Errorf("topic %s timed out with %d messages still un-sent", topic, remaining)
		}
	}
	return nil
}

// Restart producer go-routines to support Send() calls after Close() without re-initializing
// (intended for use in Lambda where Sink object may be preserved in memory for subsequent run after `Close()`)
func (s *Sink) Restart() error {
	s.prodMux.Lock()
	defer s.prodMux.Unlock()

	for topic := range s.prods {
		if err := s.prods[topic].Start(); err != nil && err != batchproducer.ErrAlreadyStarted {
			return err
		}
	}
	return nil
}

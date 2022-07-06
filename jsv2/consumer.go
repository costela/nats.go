// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"errors"

	"github.com/nats-io/nats.go"
)

type (

	// Consumer contains methods for fetching/processing messages from a stream
	Consumer interface {
		// Next is used to retrieve a single message from the stream
		Next(context.Context, ...nextOpt) (JetStreamMsg, error)
		// Stream can be used to continously receive messages and handle them with the provided callback function
		Stream(context.Context, MessageHandler, ...pullStreamOpt) error

		// Info returns Consumer details
		Info(context.Context) (*nats.ConsumerInfo, error)
	}

	// nextOpt is used to configure `Next()` method with additional parameters
	nextOpt func(*pullRequest) error

	// pullStreamOpt represent additional options used in `Stream()` for pull consumers
	pullStreamOpt func(*pullRequest) error

	// MessageHandler is a handler function used as callback in `Stream()`
	MessageHandler func(msg JetStreamMsg)

	consumer struct {
		jetStream    *jetStream
		stream       string
		durable      bool
		name         string
		subscription *nats.Subscription
		info         *nats.ConsumerInfo
	}
	pullConsumer struct {
		consumer
	}

	pullRequest struct {
		Expires   time.Duration `json:"expires,omitempty"`
		Batch     int           `json:"batch,omitempty"`
		MaxBytes  int           `json:"max_bytes,omitempty"`
		NoWait    bool          `json:"no_wait,omitempty"`
		Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
	}

	pushConsumer struct {
		consumer
	}

	pushOpts struct {
		ordered bool
	}

	// pushStreamOpt represents additional options used in `Stream()` for push consumers
	pushStreamOpt func(*pushOpts) error
)

var (
	// ErrNoMessages is returned when no messages are currectly available for a consumer
	ErrNoMessages = errors.New("nats: no messages")
	// ErrConsumerNotFound is returned when a consumer with given name is not found
	ErrConsumerNotFound = errors.New("nats: consumer not found")
	// ErrHandlerRequired is returned when no handler func is provided in Stream()
	ErrHandlerRequired = errors.New("nats: handler cannot be empty")
)

// Next fetches an individual message from a consumer.
// Timeout for this operation is handled using `context.Deadline()`, so it should always be set to avoid getting stuck
//
// Available options:
// WithNoWait() - when set to true, `Next()` request does not wait for a message if no message is available at the time of request
func (p *pullConsumer) Next(ctx context.Context, opts ...nextOpt) (JetStreamMsg, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, nats.ErrNoDeadlineContext
	}
	req := &pullRequest{
		Batch: 1,
	}
	// Make expiry a little bit shorter than timeout
	timeout := time.Until(deadline)
	if timeout >= 20*time.Millisecond {
		req.Expires = timeout - 10*time.Millisecond
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return nil, err
		}
	}
	// msgs, err := p.fetch(ctx, *req, nil)
	// if err != nil {
	// 	return nil, err
	// }
	// if len(msgs) == 0 {
	// 	return nil, ErrNoMessages
	// }
	return nil, nil
}

// Stream continously receives messages from a consumer and handles them with the provided callback function
// ctx is used to handle the whole operation, not individual messages batch, so to avoid cancellation, an empty context should be provided
//
// Available options:
// WithBatchSize() - sets a single batch request messages limit, default is set to 100
// WithExpiry() - sets a timeout for individual batch request
func (p *pullConsumer) Stream(ctx context.Context, handler MessageHandler, opts ...pullStreamOpt) error {
	if handler == nil {
		return ErrHandlerRequired
	}
	defaultTimeout := 1 * time.Second
	req := &pullRequest{
		Batch:   1000,
		Expires: defaultTimeout - 10*time.Millisecond,
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return err
		}
	}
	pending := make(chan *jetStreamMsg, 2*req.Batch)
	go func() {
	Listen:
		for {
			select {
			case <-ctx.Done():
				close(pending)
				break Listen
			default:
				if len(pending) < req.Batch {
					fetchCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
					_ = p.fetch(fetchCtx, *req, pending)
					cancel()
					// if err != nil && !errors.Is(err, nats.ErrTimeout) {

					// }
					// for _, msg := range msgs {
					// pending <- msg
					// }
				}
			}
		}
	}()

	go func() {
	Handle:
		for {
			select {
			case msg := <-pending:
				handler(msg)
			case <-ctx.Done():
				break Handle
			}
		}
	}()

	return nil
}

// fetch sends a pull request to the server and waits for messages using a subscription from `pullConsumer`
// messages will be fetched up to given batch_size or until there are no more messages or timeout is returned
func (c *pullConsumer) fetch(ctx context.Context, req pullRequest, target chan<- *jetStreamMsg) error {
	if req.Batch < 1 {
		return fmt.Errorf("%w: batch size must be at least 1", nats.ErrInvalidArg)
	}
	// if there is no subscription for this consumer, create new inbox subject and subscribe
	if c.subscription == nil {
		inbox := nats.NewInbox()
		sub, err := c.jetStream.conn.SubscribeSync(inbox)
		if err != nil {
			return err
		}
		c.subscription = sub
	}

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return err
	}

	subject := apiSubj(c.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, c.stream, c.name))
	if err := c.jetStream.conn.PublishRequest(subject, c.subscription.Subject, reqJSON); err != nil {
		return err
	}
	var count int
	for count < req.Batch {
		msg, err := c.subscription.NextMsgWithContext(ctx)
		if err != nil {
			if errors.Is(err, ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
				return nil
			}
			return err
		}
		if err := checkMsg(msg, req.NoWait); err != nil {
			if errors.Is(err, ErrNoMessages) || errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return err
		}
		target <- c.jetStream.toJSMsg(msg)
		count++
	}
	return nil
}

// Info returns nats.ConsumerInfo for a given consumer
func (p *pullConsumer) Info(ctx context.Context) (*nats.ConsumerInfo, error) {
	infoSubject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, p.stream, p.name))
	var resp consumerInfoResponse

	if _, err := p.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	p.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// toJSMsg converts core `nats.Msg` to `jetStreamMsg`, wxposing JetStream-specific operations
func (js *jetStream) toJSMsg(msg *nats.Msg) *jetStreamMsg {
	return &jetStreamMsg{
		msg:   msg,
		js:    js,
		Mutex: sync.Mutex{},
	}
}

const (
	noResponders = "503"
	noMessages   = "404"
	reqTimeout   = "408"
)

// Returns if the given message is a user message or not, and if
// `checkSts` is true, returns appropriate error based on the
// content of the status (404, etc..)
func checkMsg(msg *nats.Msg, isNoWait bool) error {
	// If payload or no header, consider this a user message
	if len(msg.Data) > 0 || len(msg.Header) == 0 {
		return nil
	}
	// Look for status header
	val := msg.Header.Get("Status")
	// If not present, then this is considered a user message
	if val == "" {
		return nil
	}

	switch val {
	case noResponders:
		return nats.ErrNoResponders
	case noMessages:
		// 404 indicates that there are no messages.
		return ErrNoMessages
	case reqTimeout:
		return nats.ErrTimeout
	}
	return fmt.Errorf("nats: %s", msg.Header.Get("Description"))
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg nats.ConsumerConfig) (Consumer, error) {
	req := createConsumerRequest{
		Stream: stream,
		Config: &cfg,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.Durable != "" {
		if err := validateDurableName(cfg.Durable); err != nil {
			return nil, err
		}
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiDurableCreateT, stream, cfg.Durable))
	} else {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateT, stream))
	}
	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, ccSubj, &resp, reqJSON); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == 10059 {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		consumer: consumer{
			jetStream: js,
			stream:    stream,
			name:      resp.Name,
			durable:   cfg.Durable != "",
			info:      resp.ConsumerInfo,
		},
	}, nil
}

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	if err := validateDurableName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerInfoT, stream, name))

	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		consumer: consumer{
			jetStream: js,
			stream:    stream,
			name:      name,
			durable:   resp.Config.Durable != "",
		},
	}, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	if err := validateDurableName(consumer); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerDeleteT, stream, consumer))

	var resp consumerDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return ErrConsumerNotFound
		}
		return resp.Error
	}
	return nil
}

// TODO: finish work on push consumer
func (c *pushConsumer) Stream(ctx context.Context, handler MessageHandler, opts ...pushStreamOpt) error {
	if handler == nil {
		return ErrHandlerRequired
	}
	var pushOpts pushOpts
	for _, opt := range opts {
		if err := opt(&pushOpts); err != nil {
			return err
		}
	}
	consumerConfig := c.info.Config
	// if pushOpts.ordered {
	// 	if c.durable {
	// 		return fmt.Errorf("nsts: durable can not be set for an ordered consumer")
	// 	}

	// 	if consumerConfig.MaxDeliver <= 1 {
	// 		return fmt.Errorf("nats: max deliver can not be set for an ordered consumer")
	// 	}

	// 	if consumerConfig.DeliverSubject != "" {

	// 	}
	// }

	if consumerConfig.DeliverSubject == "" {
		return fmt.Errorf("deliver subject cannot be empty for push-based consumers")
	}
	if consumerConfig.DeliverGroup == "" && c.info.PushBound {
		return fmt.Errorf("consumer is already bound to a subscription")
	}

	msgsChan := make(chan *nats.Msg)
	_, err := c.jetStream.conn.ChanSubscribe(consumerConfig.DeliverSubject, msgsChan)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				{
					// do something
				}
			}
		}
	}()
	// _, err := c.jetStream.conn.Subscribe(consumerConfig.DeliverSubject, handler)
	return nil
}

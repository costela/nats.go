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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type (
	JetStreamClient interface {
		Publish(context.Context, string, []byte, ...publishOpt) (*nats.PubAck, error)
		PublishMsg(context.Context, *nats.Msg, ...publishOpt) (*nats.PubAck, error)
		PublishAsync(context.Context, string, []byte, ...publishOpt) (nats.PubAckFuture, error)
		PublishMsgAsync(context.Context, *nats.Msg, ...publishOpt) (nats.PubAckFuture, error)
	}

	clientOpts struct {
		// For async publish error handling.
		aecb MsgErrHandler
		// Max async pub ack in flight
		maxpa int
	}

	publishOpt func(*pubOpts) error

	pubOpts struct {
		id             string
		lastMsgID      string  // Expected last msgId
		stream         string  // Expected stream name
		lastSeq        *uint64 // Expected last sequence
		lastSubjectSeq *uint64 // Expected last sequence per subject

		// Publish retries for NoResponders err.
		retryWait     time.Duration // Retry wait between attempts
		retryAttempts int           // Retry attempts

		// stallWait is the max wait of a async pub ack.
		stallWait time.Duration
	}

	pubAckFuture struct {
		jsClient *jetStreamClient
		msg      *nats.Msg
		ack      *nats.PubAck
		err      error
		errCh    chan error
		doneCh   chan *nats.PubAck
	}

	jetStreamClient struct {
		js *jetStream
		asyncPublishContext
		clientOpts
	}

	clientOpt func(*clientOpts) error

	// MsgErrHandler is used to process asynchronous errors from
	// JetStream PublishAsync. It will return the original
	// message sent to the server for possible retransmitting and the error encountered.
	MsgErrHandler func(JetStream, *nats.Msg, error)

	asyncPublishContext struct {
		sync.RWMutex
		replyPrefix  string
		replySubject *nats.Subscription
		acks         map[string]*pubAckFuture
		stallCh      chan struct{}
		doneCh       chan struct{}
		rr           *rand.Rand
	}

	pubAckResponse struct {
		apiResponse
		*nats.PubAck
	}
)

const (
	statusHdr = "Status"

	inboxPrefix = "_INBOX."
	rdigits     = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base        = 62
)

func (c *jetStreamClient) Publish(ctx context.Context, subj string, data []byte, opts ...publishOpt) (*nats.PubAck, error) {
	return c.PublishMsg(ctx, &nats.Msg{Subject: subj, Data: data}, opts...)
}

// PublishMsg publishes a Msg to a stream from JetStream.
func (c *jetStreamClient) PublishMsg(ctx context.Context, m *nats.Msg, opts ...publishOpt) (*nats.PubAck, error) {
	o := pubOpts{
		retryWait:     DefaultPubRetryWait,
		retryAttempts: DefaultPubRetryAttempts,
	}
	if len(opts) > 0 {
		if m.Header == nil {
			m.Header = nats.Header{}
		}
		for _, opt := range opts {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}
	if o.stallWait > 0 {
		return nil, fmt.Errorf("nats: stall wait cannot be set to sync publish")
	}

	if o.id != "" {
		m.Header.Set(MsgIDHeader, o.id)
	}
	if o.lastMsgID != "" {
		m.Header.Set(ExpectedLastMsgIDHeader, o.lastMsgID)
	}
	if o.stream != "" {
		m.Header.Set(ExpectedStreamHeader, o.stream)
	}
	if o.lastSeq != nil {
		m.Header.Set(ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}
	if o.lastSubjectSeq != nil {
		m.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}

	var resp *nats.Msg
	var err error

	resp, err = c.js.conn.RequestMsgWithContext(ctx, m)

	if err != nil {
		for r := 0; errors.Is(err, nats.ErrNoResponders) && (r < o.retryAttempts || o.retryAttempts < 0); r++ {
			// To protect against small blips in leadership changes etc, if we get a no responders here retry.
			select {
			case <-ctx.Done():
			case <-time.After(o.retryWait):
			}
			resp, err = c.js.conn.RequestMsgWithContext(ctx, m)
		}
		if err != nil {
			if errors.Is(err, nats.ErrNoResponders) {
				return nil, nats.ErrNoStreamResponse
			}
			return nil, err
		}
	}

	var ackResp pubAckResponse
	if err := json.Unmarshal(resp.Data, &ackResp); err != nil {
		return nil, nats.ErrInvalidJSAck
	}
	if ackResp.Error != nil {
		return nil, fmt.Errorf("nats: %w", ackResp.Error)
	}
	if ackResp.PubAck == nil || ackResp.PubAck.Stream == "" {
		return nil, nats.ErrInvalidJSAck
	}
	return ackResp.PubAck, nil
}

func (js *jetStreamClient) PublishAsync(ctx context.Context, subj string, data []byte, opts ...publishOpt) (nats.PubAckFuture, error) {
	return js.PublishMsgAsync(ctx, &nats.Msg{Subject: subj, Data: data}, opts...)
}

func (c *jetStreamClient) PublishMsgAsync(ctx context.Context, m *nats.Msg, opts ...publishOpt) (nats.PubAckFuture, error) {
	var o pubOpts
	if len(opts) > 0 {
		if m.Header == nil {
			m.Header = nats.Header{}
		}
		for _, opt := range opts {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}
	defaultStallWait := 200 * time.Millisecond

	stallWait := defaultStallWait
	if o.stallWait > 0 {
		stallWait = o.stallWait
	}

	if o.id != "" {
		m.Header.Set(MsgIDHeader, o.id)
	}
	if o.lastMsgID != "" {
		m.Header.Set(ExpectedLastMsgIDHeader, o.lastMsgID)
	}
	if o.stream != "" {
		m.Header.Set(ExpectedStreamHeader, o.stream)
	}
	if o.lastSeq != nil {
		m.Header.Set(ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}
	if o.lastSubjectSeq != nil {
		m.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}

	// Reply
	if m.Reply != "" {
		return nil, errors.New("nats: reply subject should be empty")
	}
	var err error
	reply := m.Reply
	m.Reply, err = c.newAsyncReply()
	if err != nil {
		return nil, fmt.Errorf("nats: error creating async reply handler: %s", err)
	}
	defer func() { m.Reply = reply }()

	id := m.Reply[aReplyPreLen:]
	paf := &pubAckFuture{msg: m}
	numPending, maxPending := c.registerPAF(id, paf)

	if maxPending > 0 && numPending >= maxPending {
		select {
		case <-c.asyncStall():
		case <-time.After(stallWait):
			c.clearPAF(id)
			return nil, errors.New("nats: stalled with too many outstanding async published messages")
		}
	}
	if err := c.js.conn.PublishMsg(m); err != nil {
		c.clearPAF(id)
		return nil, err
	}

	return paf, nil
}

// For quick token lookup etc.
const (
	aReplyPreLen    = 14
	aReplyTokensize = 6
)

func (c *jetStreamClient) newAsyncReply() (string, error) {
	c.Lock()
	if c.replySubject == nil {
		// Create our wildcard reply subject.
		sha := sha256.New()
		sha.Write([]byte(nuid.Next()))
		b := sha.Sum(nil)
		for i := 0; i < aReplyTokensize; i++ {
			b[i] = rdigits[int(b[i]%base)]
		}
		c.replyPrefix = fmt.Sprintf("%s%s.", inboxPrefix, b[:aReplyTokensize])
		sub, err := c.js.conn.Subscribe(fmt.Sprintf("%s*", c.replyPrefix), c.handleAsyncReply)
		if err != nil {
			c.Unlock()
			return "", err
		}
		c.replySubject = sub
		c.rr = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	var sb strings.Builder
	sb.WriteString(c.replyPrefix)
	rn := c.rr.Int63()
	var b [aReplyTokensize]byte
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = rdigits[l%base]
		l /= base
	}
	sb.Write(b[:])
	c.Unlock()
	return sb.String(), nil
}

// Handle an async reply from PublishAsync.
func (c *jetStreamClient) handleAsyncReply(m *nats.Msg) {
	if len(m.Subject) <= aReplyPreLen {
		return
	}
	id := m.Subject[aReplyPreLen:]

	c.Lock()
	paf := c.getPAF(id)
	if paf == nil {
		c.Unlock()
		return
	}
	// Remove
	delete(c.acks, id)

	// Check on anyone stalled and waiting.
	if c.stallCh != nil && len(c.acks) < c.clientOpts.maxpa {
		close(c.stallCh)
		c.stallCh = nil
	}
	// Check on anyone waiting on done status.
	if c.doneCh != nil && len(c.acks) == 0 {
		dch := c.doneCh
		c.doneCh = nil
		// Defer here so error is processed and can be checked.
		defer close(dch)
	}

	doErr := func(err error) {
		paf.err = err
		if paf.errCh != nil {
			paf.errCh <- paf.err
		}
		cb := c.clientOpts.aecb
		c.Unlock()
		if cb != nil {
			cb(c.js, paf.msg, err)
		}
	}

	// Process no responders etc.
	if len(m.Data) == 0 && m.Header.Get(statusHdr) == noResponders {
		doErr(nats.ErrNoResponders)
		return
	}

	var pa pubAckResponse
	if err := json.Unmarshal(m.Data, &pa); err != nil {
		doErr(nats.ErrInvalidJSAck)
		return
	}
	if pa.Error != nil {
		doErr(fmt.Errorf("nats: %s", pa.Error))
		return
	}
	if pa.PubAck == nil || pa.PubAck.Stream == "" {
		doErr(nats.ErrInvalidJSAck)
		return
	}

	// So here we have received a proper puback.
	paf.ack = pa.PubAck
	if paf.doneCh != nil {
		paf.doneCh <- paf.ack
	}
	c.Unlock()
}

// registerPAF will register for a PubAckFuture.
func (c *jetStreamClient) registerPAF(id string, paf *pubAckFuture) (int, int) {
	c.Lock()
	if c.acks == nil {
		c.acks = make(map[string]*pubAckFuture)
	}
	paf.jsClient = c
	c.acks[id] = paf
	np := len(c.acks)
	maxpa := c.clientOpts.maxpa
	c.Unlock()
	return np, maxpa
}

// Lock should be held.
func (c *jetStreamClient) getPAF(id string) *pubAckFuture {
	if c.acks == nil {
		return nil
	}
	return c.acks[id]
}

// clearPAF will remove a PubAckFuture that was registered.
func (c *jetStreamClient) clearPAF(id string) {
	c.Lock()
	delete(c.acks, id)
	c.Unlock()
}

func (c *jetStreamClient) asyncStall() <-chan struct{} {
	c.Lock()
	if c.stallCh == nil {
		c.stallCh = make(chan struct{})
	}
	stc := c.stallCh
	c.Unlock()
	return stc
}

func (paf *pubAckFuture) Ok() <-chan *nats.PubAck {
	paf.jsClient.Lock()
	defer paf.jsClient.Unlock()

	if paf.doneCh == nil {
		paf.doneCh = make(chan *nats.PubAck, 1)
		if paf.ack != nil {
			paf.doneCh <- paf.ack
		}
	}

	return paf.doneCh
}

func (paf *pubAckFuture) Err() <-chan error {
	paf.jsClient.Lock()
	defer paf.jsClient.Unlock()

	if paf.errCh == nil {
		paf.errCh = make(chan error, 1)
		if paf.err != nil {
			paf.errCh <- paf.err
		}
	}

	return paf.errCh
}

func (paf *pubAckFuture) Msg() *nats.Msg {
	paf.jsClient.RLock()
	defer paf.jsClient.RUnlock()
	return paf.msg
}

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	JetStreamMsg interface {
		JetStreamMessageReader
		Ack(...ackOpt) error
		DoubleAck(context.Context, ...ackOpt) error
		Nak(...nakOpt) error
		InProgress(...ackOpt) error
		Term(...ackOpt) error
	}

	JetStreamMessageReader interface {
		Metadata() (*nats.MsgMetadata, error)
		Data() []byte
		Headers() nats.Header
		Subject() string
	}

	jetStreamMsg struct {
		msg  *nats.Msg
		ackd bool
		js   *jetStream
		sync.Mutex
	}

	ackOpts struct {
		nakDelay time.Duration
		ctx      context.Context
	}

	ackOpt func(*ackOpts) error

	nakOpt func(*ackOpts) error

	ackType []byte
)

const (
	ackDomainTokenPos = iota + 2
	ackAccHashTokenPos
	ackStreamTokenPos
	ackConsumerTokenPos
	ackNumDeliveredTokenPos
	ackStreamSeqTokenPos
	ackConsumerSeqTokenPos
	ackTimestampSeqTokenPos
	ackNumPendingTokenPos
)

var (
	ErrNotJSMessage   = errors.New("nats: not a jetstream message")
	ErrMsgAlreadyAckd = errors.New("nats: message was already acknowledged")
	ErrMsgNotBound    = errors.New("nats: message is not bound to subscription/connection")
	ErrMsgNoReply     = errors.New("nats: message does not have a reply")
)

var (
	ackAck      ackType = []byte("+ACK")
	ackNak      ackType = []byte("-NAK")
	ackProgress ackType = []byte("+WPI")
	ackTerm     ackType = []byte("+TERM")
)

func (m *jetStreamMsg) Metadata() (*nats.MsgMetadata, error) {
	if err := m.checkReply(); err != nil {
		return nil, err
	}

	tokens, err := getMetadataFields(m.msg.Reply)
	if err != nil {
		return nil, err
	}

	meta := &nats.MsgMetadata{
		Domain:       tokens[ackDomainTokenPos],
		NumDelivered: uint64(parseNum(tokens[ackNumDeliveredTokenPos])),
		NumPending:   uint64(parseNum(tokens[ackNumPendingTokenPos])),
		Timestamp:    time.Unix(0, parseNum(tokens[ackTimestampSeqTokenPos])),
		Stream:       tokens[ackStreamTokenPos],
		Consumer:     tokens[ackConsumerTokenPos],
	}
	meta.Sequence.Stream = uint64(parseNum(tokens[ackStreamSeqTokenPos]))
	meta.Sequence.Consumer = uint64(parseNum(tokens[ackConsumerSeqTokenPos]))
	return meta, nil
}

// Quick parser for positive numbers in ack reply encoding.
func parseNum(d string) (n int64) {
	if len(d) == 0 {
		return -1
	}

	// ASCII numbers 0-9
	const (
		asciiZero = 48
		asciiNine = 57
	)

	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}

func getMetadataFields(subject string) ([]string, error) {
	v1TokenCounts, v2TokenCounts := 9, 12

	var start int
	tokens := make([]string, 0, v2TokenCounts)
	for i := 0; i < len(subject); i++ {
		if subject[i] == '.' {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	//
	// Newer server will include the domain name and account hash in the subject,
	// and a token at the end.
	//
	// Old subject was:
	// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
	//
	// New subject would be:
	// $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>
	//
	// v1 has 9 tokens, v2 has 12, but we must not be strict on the 12th since
	// it may be removed in the future. Also, the library has no use for it.
	// The point is that a v2 ACK subject is valid if it has at least 11 tokens.
	//
	tokensLen := len(tokens)
	// If lower than 9 or more than 9 but less than 11, report an error
	if tokensLen < v1TokenCounts || (tokensLen > v1TokenCounts && tokensLen < v2TokenCounts-1) {
		return nil, fmt.Errorf("%w: invalid format of ACK subject", ErrNotJSMessage)
	}
	if tokens[0] != "$JS" || tokens[1] != "ACK" {
		return nil, fmt.Errorf("%w: invalid format of ACK subject", ErrNotJSMessage)
	}
	// For v1 style, we insert 2 empty tokens (domain and hash) so that the
	// rest of the library references known fields at a constant location.
	if tokensLen == v1TokenCounts {
		// Extend the array (we know the backend is big enough)
		tokens = append(tokens[:ackDomainTokenPos+2], tokens[ackDomainTokenPos:]...)
		// Clear the domain and hash tokens
		tokens[ackDomainTokenPos], tokens[ackAccHashTokenPos] = "", ""

	} else if tokens[ackDomainTokenPos] == "_" {
		// If domain is "_", replace with empty value.
		tokens[ackDomainTokenPos] = ""
	}
	return tokens, nil
}

func (m *jetStreamMsg) Data() []byte {
	return m.msg.Data
}

func (m *jetStreamMsg) Headers() nats.Header {
	return m.msg.Header
}

func (m *jetStreamMsg) Subject() string {
	return m.msg.Subject
}

func (m *jetStreamMsg) Ack(opts ...ackOpt) error {
	return m.ackReply(ackAck, false, ackOpts{})
}

func (m *jetStreamMsg) DoubleAck(ctx context.Context, opts ...ackOpt) error {
	o := ackOpts{
		ctx: ctx,
	}
	return m.ackReply(ackAck, true, o)
}

func (m *jetStreamMsg) Nak(opts ...nakOpt) error {
	var o ackOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	return m.ackReply(ackNak, false, o)
}

func (m *jetStreamMsg) InProgress(_ ...ackOpt) error {
	return m.ackReply(ackProgress, false, ackOpts{})
}

func (m *jetStreamMsg) Term(_ ...ackOpt) error {
	return m.ackReply(ackTerm, false, ackOpts{})
}

func (m *jetStreamMsg) ackReply(ackType ackType, sync bool, opts ackOpts) error {
	err := m.checkReply()
	if err != nil {
		return err
	}

	m.Lock()
	if m.ackd {
		return ErrMsgAlreadyAckd
	}
	m.Unlock()

	var hasDeadline bool
	if opts.ctx != nil {
		_, hasDeadline = opts.ctx.Deadline()
	}
	if sync && !hasDeadline {
		return fmt.Errorf("for synchronous acknowledgements, context with deadline has to be provided")
	}
	if opts.nakDelay != 0 && !bytes.Equal(ackType, ackNak) {
		return fmt.Errorf("delay can only be set for NAK")
	}

	var body []byte
	if opts.nakDelay > 0 {
		body = []byte(fmt.Sprintf("%s {\"delay\": %d}", ackType, opts.nakDelay.Nanoseconds()))
	} else {
		body = ackType
	}

	if sync {
		_, err = m.js.conn.RequestWithContext(opts.ctx, m.msg.Reply, body)
	} else {
		err = m.js.conn.Publish(m.msg.Reply, body)
	}
	if err != nil {
		return err
	}

	// Mark that the message has been acked unless it is ackProgress
	// which can be sent many times.
	if !bytes.Equal(ackType, ackProgress) {
		m.Lock()
		m.ackd = true
		m.Unlock()
	}
	return nil
}

func (m *jetStreamMsg) checkReply() error {
	if m == nil || m.msg.Sub == nil {
		return ErrMsgNotBound
	}
	if m.msg.Reply == "" {
		return ErrMsgNoReply
	}
	return nil
}

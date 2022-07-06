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
	"errors"
	"fmt"
	"time"
)

// ErrInvalidOption is returned when there is a collision between options
var ErrInvalidOption = errors.New("jetstream: invalid option")

// WithClientTrace enables request/responce API calls tracing
// ClientTrace is used to provide handlers for each event
func WithClientTrace(ct *ClientTrace) jetStreamOpt {
	return func(opts *jsOpts) error {
		opts.clientTrace = ct
		return nil
	}
}

// WithSubject sets a sprecific subject for which messages on a stream will be purged
func WithSubject(subject string) purgeOpt {
	return func(req *streamPurgeRequest) error {
		req.Subject = subject
		return nil
	}
}

// WithSequence is used to set a sprecific sequence number up to which (but not including) messages will be purged from a stream
// Can be combined with `WithSubject()` option, but not with `WithKeep()`
func WithSequence(sequence uint64) purgeOpt {
	return func(req *streamPurgeRequest) error {
		if req.Keep != 0 {
			return fmt.Errorf("%w: both 'keep' and 'sequence' cannot be provided in purge request", ErrInvalidOption)
		}
		req.Sequence = sequence
		return nil
	}
}

// WithKeep sets the number of messages to be kept in the stream after purge.
// Can be combined with `WithSubject()` option, but not with `WithSequence()`
func WithKeep(keep uint64) purgeOpt {
	return func(req *streamPurgeRequest) error {
		if req.Sequence != 0 {
			return fmt.Errorf("%w: both 'keep' and 'sequence' cannot be provided in purge request", ErrInvalidOption)
		}
		req.Keep = keep
		return nil
	}
}

// WithNoWait can be used to terminate the 'Next()` request and return ErrNoMessages immediately if there are no messages at the time of request
func WithNoWait(noWait bool) nextOpt {
	return func(cfg *pullRequest) error {
		cfg.NoWait = noWait
		return nil
	}
}

// WithBatchSize limits the number of messages to be fetched from the stream in one request
// If not provided, a default of 100 messages will be used
func WithBatchSize(batch int) pullStreamOpt {
	return func(cfg *pullRequest) error {
		cfg.Batch = batch
		return nil
	}
}

// WithExpiry sets timeount on a single batch request, waiting until at least one message is available
func WithExpiry(expires time.Duration) pullStreamOpt {
	return func(cfg *pullRequest) error {
		cfg.Expires = expires
		return nil
	}
}

// WithDeletedDetails can be used to display the information about messages deleted from a stream on a stream info request
func WithDeletedDetails(deletedDetails bool) streamInfoOpt {
	return func(req *streamInfoRequest) error {
		req.DeletedDetails = deletedDetails
		return nil
	}
}

func WithNakDelay(delay time.Duration) ackOpt {
	return func(opts *ackOpts) error {
		opts.nakDelay = delay
		return nil
	}
}

// MsgId sets the message ID used for deduplication.
func WithMsgID(id string) publishOpt {
	return func(opts *pubOpts) error {
		opts.id = id
		return nil
	}
}

// ExpectStream sets the expected stream to respond from the publish.
func WithExpectStream(stream string) publishOpt {
	return func(opts *pubOpts) error {
		opts.stream = stream
		return nil
	}
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func WithExpectLastSequence(seq uint64) publishOpt {
	return func(opts *pubOpts) error {
		opts.lastSeq = &seq
		return nil
	}
}

// ExpectLastSequencePerSubject sets the expected sequence per subject in the response from the publish.
func WithExpectLastSequencePerSubject(seq uint64) publishOpt {
	return func(opts *pubOpts) error {
		opts.lastSubjectSeq = &seq
		return nil
	}
}

// ExpectLastMsgId sets the expected last msgId in the response from the publish.
func WithExpectLastMsgID(id string) publishOpt {
	return func(opts *pubOpts) error {
		opts.lastMsgID = id
		return nil
	}
}

// RetryWait sets the retry wait time when ErrNoResponders is encountered.
func WithRetryWait(dur time.Duration) publishOpt {
	return func(opts *pubOpts) error {
		opts.retryWait = dur
		return nil
	}
}

// RetryAttempts sets the retry number of attempts when ErrNoResponders is encountered.
func WithRetryAttempts(num int) publishOpt {
	return func(opts *pubOpts) error {
		opts.retryAttempts = num
		return nil
	}
}

// StallWait sets the max wait when the producer becomes stall producing messages.
func WithStallWait(ttl time.Duration) publishOpt {
	return func(opts *pubOpts) error {
		if ttl <= 0 {
			return fmt.Errorf("nats: stall wait should be more than 0")
		}
		opts.stallWait = ttl
		return nil
	}
}

func WithPublishAsyncErrHandler(cb MsgErrHandler) clientOpt {
	return func(opts *clientOpts) error {
		opts.aecb = cb
		return nil
	}
}

func WithIdleHeartbeat(hb time.Duration) pullStreamOpt {
	return func(req *pullRequest) error {
		if hb <= 0 {
			return fmt.Errorf("idle_heartbeat value must be greater than 0")
		}
		req.Heartbeat = hb
		return nil
	}
}

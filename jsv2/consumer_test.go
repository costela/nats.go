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
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestPullConsumerNext(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	tests := []struct {
		name           string
		deadline       time.Duration
		consumerConfig nats.ConsumerConfig
		opts           []nextOpt
		expectedMsgs   []string
		withError      error
	}{
		{
			name:           "fetch messages from a subject",
			deadline:       2 * time.Second,
			consumerConfig: nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy},
			expectedMsgs:   testMsgs,
		},
		{
			name:           "fetch with no wait",
			deadline:       2 * time.Second,
			consumerConfig: nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy},
			opts:           []nextOpt{WithNoWait(true)},
			expectedMsgs:   testMsgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, srv)
			nc, err := nats.Connect(srv.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.AddConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			publishTestMsgs(t, nc)

			msgs := make([]JetStreamMsg, 0)
			var nextErr error
			var msg JetStreamMsg
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, nextErr = c.Next(nextCtx, test.opts...)
				cancel()
				if nextErr != nil {
					break
				}
				msgs = append(msgs, msg)
			}
			if test.withError != nil {
				if nextErr == nil {
					t.Fatalf("Expected error: %s; got nil", nextErr)
				}
				if !errors.Is(nextErr, test.withError) {
					t.Fatalf("Expected error: %s; got: %s", test.withError, nextErr)
				}
				return
			}
			if nextErr != nil && !errors.Is(nextErr, ErrNoMessages) {
				t.Fatalf("Unexpected error: %s", nextErr)
			}
			if len(msgs) != len(test.expectedMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(test.expectedMsgs), len(msgs))
			}
			for i, msg := range msgs {
				if string(msg.Data()) != test.expectedMsgs[i] {
					t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, test.expectedMsgs[i], string(msg.Data()))
				}
			}
		})
	}

}

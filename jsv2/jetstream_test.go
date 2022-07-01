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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestAddStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	defer nc.Close()

	_, err = js.Stream(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected no error: %v", err)
	}

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	cons, err := s.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	go func() {
		nextCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := 0; i < 15; i++ {
			msg, err := cons.Next(nextCtx, WithNoWait(false))
			if err != nil {
				if errors.Is(err, ErrNoMessages) {
					fmt.Println("no messages!")
					break
				}
				t.Fatalf("Unexpected error: %v", err)
			}
			fmt.Println("Receievd message: " + string(msg.Data()))
		}
	}()

	time.Sleep(2 * time.Second)

	for i := 0; i < 14; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	time.Sleep(10 * time.Second)

	// fmt.Println("consumer name: %s", cons.)
}

func TestNext(t *testing.T) {
	opts := []nats.Option{nats.Name("JetStream Consumer Management")}
	opts = append(opts, nats.UserCredentials("/Users/piotrpiotrowski/.local/share/nats/nsc/keys/creds/synadia/piotrpio/default.creds"))
	nc, err := nats.Connect("tls://connect.ngs.global:4222", opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	stream, err := js.Stream(ctx, "TEST_STREAM")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cons, err := stream.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy, Description: "abc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// for i := 0; i < 5; i++ {
	// 	msg, err := cons.Next(ctx, WithNoWait(true))
	// 	if err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// 	if msg != nil {
	// 		if err := msg.Ack(ctx); err != nil {
	// 			t.Fatalf("Unexpected error: %v", err)
	// 		}
	// 	}

	// 	fmt.Printf("Receievd message: %s\n", string(msg.Data()))
	// }

	err = cons.Stream(ctx, func(msg JetStreamMsg) {
		fmt.Println(string(msg.Data()))
		if err := msg.Ack(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	time.Sleep(10 * time.Second)
}

func TestStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	_, err = js.Stream(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected no error: %v", err)
	}

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	cons, err := s.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 30; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	err = cons.Stream(ctx, func(msg JetStreamMsg) {
		fmt.Println(string(msg.Data()))
	})
	for i := 0; i < 30; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(10 * time.Second)

	// for i := 0; i < 10; i++ {
	// 	if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("second batch msg %d", i))); err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// }

	// time.Sleep(5 * time.Second)
}

func TestStream_Timeout(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	_, err = js.Stream(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected no error: %v", err)
	}

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	cons, err := s.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(2 * time.Second)

	err = cons.Stream(ctx, func(msg JetStreamMsg) {
		fmt.Println(string(msg.Data()))
	}, WithBatchSize(100), WithExpiry(5*time.Second))

	go func() {
		var i int
		for ; ; i++ {
			time.Sleep(100 * time.Millisecond)
			if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
	}()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(20 * time.Second)

	// for i := 0; i < 10; i++ {
	// 	if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("second batch msg %d", i))); err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// }

	// time.Sleep(5 * time.Second)
}

func TestListen_WithHeartbeat(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	_, err = js.Stream(ctx, "foo")
	if err == nil {
		t.Fatalf("Expected no error: %v", err)
	}

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	cons, err := s.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 30; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	err = cons.Stream(ctx, func(msg JetStreamMsg) {
		fmt.Println(string(msg.Data()))
	}, WithIdleHeartbeat(300*time.Millisecond))

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(25 * time.Second)

	// for i := 0; i < 10; i++ {
	// 	if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("second batch msg %d", i))); err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// }

	// time.Sleep(5 * time.Second)
}

func TestPurgeStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	defer nc.Close()

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 14; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("Before purge: %d\n", info.State.Msgs)

	err = s.Purge(ctx, WithSequence(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("After purge: %d\n", info.State.Msgs)

}

func TestAccountInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	defer nc.Close()
	_, err = js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err := js.AccountInfo(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("Max memory: %d\n", info.Limits.MaxMemory)
	fmt.Printf("Streams: %d\n", info.Streams)

}

func TestPublishStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// _, err = js.Stream(ctx, "foo")
	// if err == nil {
	// 	t.Fatalf("Expected no error: %v", err)
	// }

	s, err := js.AddStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Println(info.Config.Name)
	fmt.Println(info.Config.Description)

	cons, err := s.AddConsumer(ctx, nats.ConsumerConfig{Durable: "dupa", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	client, err := js.Client(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 14; i++ {
		ack, err := client.Publish(ctx, "FOO.123", []byte(fmt.Sprintf("msg %d", i)))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fmt.Printf("Published msg with seq: %d on stream %s\n", ack.Sequence, ack.Stream)
	}

	time.Sleep(2 * time.Second)

	err = cons.Stream(ctx, func(msg JetStreamMsg) {
		fmt.Println(string(msg.Data()))
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < 14; i++ {
		ack, err := client.Publish(ctx, "FOO.123", []byte(fmt.Sprintf("second batch msg %d", i)))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fmt.Printf("Published msg with seq: %d on stream %s\n", ack.Sequence, ack.Stream)
	}

	time.Sleep(5 * time.Second)
}

func BenchmarkGetMetadataFields(b *testing.B) {
	sub := "$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>"
	for n := 0; n < b.N; n++ {
		getMetadataFields(sub)
	}
}

func TestGetMetadataFields(t *testing.T) {
	sub := "$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>"
	res, err := getMetadataFields(sub)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := []string{"$JS", "ACK", "", "", "<stream>", "<consumer>", "<delivered>", "<sseq>", "<cseq>", "<tm>", "<pending>"}
	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("\nExpected: %v\nGot:%v", expected, res)
	}
}

func TestJetStreamPublishAsync(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := GetJetStream(ctx, nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	client, err := js.Client(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Make sure we get a proper failure when no stream is present.
	paf, err := client.PublishAsync(ctx, "foo", []byte("Hello JS"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case <-paf.Ok():
		t.Fatalf("Did not expect to get PubAck")
	case err := <-paf.Err():
		if err != nats.ErrNoResponders {
			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
		}
		// Should be able to get the message back to resend, etc.
		m := paf.Msg()
		if m == nil {
			t.Fatalf("Expected to be able to retrieve the message")
		}
		if m.Subject != "foo" || string(m.Data) != "Hello JS" {
			t.Fatalf("Wrong message: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive an error in time")
	}

	// Now create a stream and expect a PubAck from <-OK().
	if _, err := js.AddStream(ctx, nats.StreamConfig{Name: "TEST"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	paf, err = client.PublishAsync(ctx, "TEST", []byte("Hello JS ASYNC PUB"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case pa := <-paf.Ok():
		if pa.Stream != "TEST" || pa.Sequence != 1 {
			t.Fatalf("Bad PubAck: %+v", pa)
		}
	case err := <-paf.Err():
		t.Fatalf("Did not expect to get an error: %v", err)
	case <-time.After(time.Second):
		t.Fatalf("Did not receive a PubAck in time")
	}

	errCh := make(chan error, 1)

	// Make sure we can register an async err handler for these.
	errHandler := func(js JetStream, originalMsg *nats.Msg, err error) {
		if originalMsg == nil {
			t.Fatalf("Expected non-nil original message")
		}
		errCh <- err
	}

	client, err = js.Client(ctx, WithPublishAsyncErrHandler(errHandler))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err = client.PublishAsync(ctx, "bar", []byte("Hello JS ASYNC PUB")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nats.ErrNoResponders {
			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive an async err in time")
	}

	// // Now test that we can set our window for the JetStream context to limit number of outstanding messages.
	// js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	// if err != nil {
	// 	t.Fatalf("Unexpected error: %v", err)
	// }

	// for i := 0; i < 100; i++ {
	// 	if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// 	if np := js.PublishAsyncPending(); np > 10 {
	// 		t.Fatalf("Expected num pending to not exceed 10, got %d", np)
	// 	}
	// }

	// // Now test that we can wait on all prior outstanding if we want.
	// js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	// if err != nil {
	// 	t.Fatalf("Unexpected error: %v", err)
	// }

	// for i := 0; i < 500; i++ {
	// 	if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
	// 		t.Fatalf("Unexpected error: %v", err)
	// 	}
	// }

	// select {
	// case <-js.PublishAsyncComplete():
	// case <-time.After(5 * time.Second):
	// 	t.Fatalf("Did not receive completion signal")
	// }

	// // Check invalid options
	// _, err = js.PublishAsync("foo", []byte("Bad"), nats.StallWait(0))
	// expectedErr := "nats: stall wait should be more than 0"
	// if err == nil || err.Error() != expectedErr {
	// 	t.Errorf("Expected %v, got: %v", expectedErr, err)
	// }

	// _, err = js.Publish("foo", []byte("Also bad"), nats.StallWait(200*time.Millisecond))
	// expectedErr = "nats: stall wait cannot be set to sync publish"
	// if err == nil || err.Error() != expectedErr {
	// 	t.Errorf("Expected %v, got: %v", expectedErr, err)
	// }
}

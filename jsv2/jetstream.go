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
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
)

type (

	// JetStream contains CRUD methods to operate on a stream
	// Create, update and get operations return 'Stream' interface,
	// allowing operations on consumers
	//
	// AddConsumer, Consumer and DeleteConsumer are helper methods used to create/fetch/remove consumer without fetching stream (bypassing stream API)
	//
	// Client returns a JetStremClient, used to publish messages on a stream or fetch messages by sequence number
	JetStream interface {
		// Returns *nats.AccountInfo, containing details about the account associated with this JetStream connection
		AccountInfo(ctx context.Context) (*nats.AccountInfo, error)

		// AddStream creates a new stream with given config and returns a hook to operate on it
		AddStream(context.Context, nats.StreamConfig) (Stream, error)
		// UpdateStream updates an existing stream
		UpdateStream(context.Context, nats.StreamConfig) (Stream, error)
		// Stream returns a `Stream` hook for a given stream name
		Stream(context.Context, string) (Stream, error)
		// DeleteStream removes a stream with given name
		DeleteStream(context.Context, string) error

		// AddConsumer creates a consumer on a given stream with given config
		// This operation is idempotent - if a consumer already exists, it will be a no-op (or error if configs do not match)
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		AddConsumer(context.Context, string, nats.ConsumerConfig) (Consumer, error)
		// Consumer returns a hook to an existing consumer, allowing processing of messages
		Consumer(context.Context, string, string) (Consumer, error)
		// DeleteConsumer removes a consumer with given name from a stream
		DeleteConsumer(context.Context, string, string) error

		// Client returns a JetStreamClient interface, allowing publishing/fetching JetStream messages
		Client(context.Context, ...clientOpt) (JetStreamClient, error)
	}

	jetStream struct {
		conn *nats.Conn
		jsOpts

		publisher *jetStreamClient
	}

	jetStreamOpt func(*jsOpts) error

	jsOpts struct {
		apiPrefix   string
		clientTrace *ClientTrace
	}

	// ClientTrace can be used to trace API interactions for the JetStream Context.
	ClientTrace struct {
		RequestSent      func(subj string, payload []byte)
		ResponseReceived func(subj string, payload []byte, hdr nats.Header)
	}
	streamInfoResponse struct {
		apiResponse
		*nats.StreamInfo
	}

	accountInfoResponse struct {
		apiResponse
		nats.AccountInfo
	}

	streamDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}
)

const (
	MsgIDHeader               = "Nats-Msg-Id"
	ExpectedStreamHeader      = "Nats-Expected-Stream"
	ExpectedLastSeqHeader     = "Nats-Expected-Last-Sequence"
	ExpectedLastSubjSeqHeader = "Nats-Expected-Last-Subject-Sequence"
	ExpectedLastMsgIDHeader   = "Nats-Expected-Last-Msg-Id"
	MsgRollup                 = "Nats-Rollup"
)

var (
	ErrJetStreamNotEnabled    = errors.New("nats: jetstream not enabled")
	ErrJetStreamBadPre        = errors.New("nats: jetstream api prefix not valid")
	ErrStreamNameAlreadyInUse = errors.New("nats: stream name already in use")
	ErrStreamNameRequired     = errors.New("nats: stream name is required")
	ErrInvalidStreamName      = errors.New("nats: invalid stream name")
)

func GetJetStream(ctx context.Context, nc *nats.Conn, opts ...jetStreamOpt) (JetStream, error) {
	var jsOpts jsOpts
	for _, opt := range opts {
		if err := opt(&jsOpts); err != nil {
			return nil, err
		}
	}
	if jsOpts.apiPrefix == "" {
		jsOpts.apiPrefix = defaultAPIPrefix
	}

	js := &jetStream{
		conn:   nc,
		jsOpts: jsOpts,
	}

	return js, nil
}

func (js *jetStream) AddStream(ctx context.Context, cfg nats.StreamConfig) (Stream, error) {
	if err := validateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	createSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamCreateT, cfg.Name))
	var resp streamInfoResponse

	if _, err = js.apiRequestJSON(ctx, createSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == 10058 {
			return nil, ErrStreamNameAlreadyInUse
		}
		return nil, resp.Error
	}

	return &stream{
		jetStream: js,
		name:      cfg.Name,
	}, nil
}

func (js *jetStream) UpdateStream(ctx context.Context, cfg nats.StreamConfig) (Stream, error) {
	if err := validateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	updateSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamUpdateT, cfg.Name))
	var resp streamInfoResponse

	if _, err = js.apiRequestJSON(ctx, updateSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}

	return &stream{
		jetStream: js,
		name:      cfg.Name,
	}, nil
}

func (js *jetStream) Stream(ctx context.Context, name string) (Stream, error) {
	if err := validateStreamName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamInfoT, name))

	var resp streamInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return &stream{
		jetStream: js,
		name:      name,
	}, nil
}

func (js *jetStream) DeleteStream(ctx context.Context, name string) error {
	if err := validateStreamName(name); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamDeleteT, name))
	var resp streamDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return ErrStreamNotFound
		}
		return resp.Error
	}
	return nil
}

func (js *jetStream) AddConsumer(ctx context.Context, stream string, cfg nats.ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, js, stream, cfg)
}

func (js *jetStream) Consumer(ctx context.Context, stream string, name string) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return getConsumer(ctx, js, stream, name)
}

func (js *jetStream) DeleteConsumer(ctx context.Context, stream string, name string) error {
	if err := validateStreamName(stream); err != nil {
		return err
	}
	return deleteConsumer(ctx, js, stream, name)
}

func validateStreamName(stream string) error {
	if stream == "" {
		return ErrStreamNameRequired
	}
	if strings.Contains(stream, ".") {
		return fmt.Errorf("%s: '%s'", ErrInvalidStreamName, stream)
	}
	return nil
}

func (js *jetStream) AccountInfo(ctx context.Context) (*nats.AccountInfo, error) {
	var resp accountInfoResponse

	infoSubject := apiSubj(js.apiPrefix, apiAccountInfo)
	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			err = ErrJetStreamNotEnabled
		}
		return nil, err
	}
	if resp.Error != nil {
		if strings.Contains(resp.Error.Description, "not enabled for") {
			return nil, ErrJetStreamNotEnabled
		}
		return nil, resp.Error
	}

	return &resp.AccountInfo, nil
}

func (js *jetStream) Client(ctx context.Context, opts ...clientOpt) (JetStreamClient, error) {
	var clientOpts clientOpts
	for _, opt := range opts {
		if err := opt(&clientOpts); err != nil {
			return nil, err
		}
	}
	if js.publisher == nil {
		js.publisher = &jetStreamClient{
			js:         js,
			clientOpts: clientOpts,
		}
		return js.publisher, nil
	}
	js.publisher.clientOpts = clientOpts
	return js.publisher, nil
}

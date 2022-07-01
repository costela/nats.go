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
	/*
		Stream contains CRUD operations on consumers
		Create, update and get operations return 'Consumer' interface,
		allowing fetching/processing of messages

		Info fetches StreamInfo from the API
	*/
	Stream interface {
		streamConsumerManager
		Info(context.Context, ...streamInfoOpt) (*nats.StreamInfo, error)
		Purge(context.Context, ...purgeOpt) error
	}

	streamConsumerManager interface {
		AddConsumer(context.Context, nats.ConsumerConfig) (Consumer, error)
		UpdateConsumer(context.Context, nats.ConsumerConfig) (Consumer, error)
		Consumer(context.Context, string) (Consumer, error)
		DeleteConsumer(context.Context, string) error
	}

	stream struct {
		name      string
		jetStream *jetStream
	}

	streamInfoOpt func(*streamInfoRequest) error

	streamInfoRequest struct {
		DeletedDetails bool `json:"deleted_details,omitempty"`
	}

	consumerInfoResponse struct {
		apiResponse
		*nats.ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string               `json:"stream_name"`
		Config *nats.ConsumerConfig `json:"config"`
	}

	purgeOpt func(*streamPurgeRequest) error

	streamPurgeRequest struct {
		// Purge up to but not including sequence.
		Sequence uint64 `json:"seq,omitempty"`
		// Subject to match against messages for the purge command.
		Subject string `json:"filter,omitempty"`
		// Number of messages to keep.
		Keep uint64 `json:"keep,omitempty"`
	}

	streamPurgeResponse struct {
		apiResponse
		Success bool   `json:"success,omitempty"`
		Purged  uint64 `json:"purged"`
	}

	consumerDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}
)

var (
	ErrStreamNotFound     = errors.New("nats: stream not found")
	ErrInvalidDurableName = errors.New("nats: invalid durable name")
)

func (s *stream) AddConsumer(ctx context.Context, cfg nats.ConsumerConfig) (Consumer, error) {
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) UpdateConsumer(ctx context.Context, cfg nats.ConsumerConfig) (Consumer, error) {
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) Consumer(ctx context.Context, name string) (Consumer, error) {
	return getConsumer(ctx, s.jetStream, s.name, name)
}

func (s *stream) DeleteConsumer(ctx context.Context, name string) error {
	return deleteConsumer(ctx, s.jetStream, s.name, name)
}

func (s *stream) Info(ctx context.Context, opts ...streamInfoOpt) (*nats.StreamInfo, error) {
	var infoReq *streamInfoRequest
	for _, opt := range opts {
		if infoReq == nil {
			infoReq = &streamInfoRequest{}
		}
		if err := opt(infoReq); err != nil {
			return nil, err
		}
	}
	var req []byte
	var err error
	if req != nil {
		req, err = json.Marshal(infoReq)
		if err != nil {
			return nil, err
		}
	}

	infoSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamInfoT, s.name))
	var resp streamInfoResponse

	if _, err = s.jetStream.apiRequestJSON(ctx, infoSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return resp.StreamInfo, nil
}

func (s *stream) Purge(ctx context.Context, opts ...purgeOpt) error {
	var purgeReq streamPurgeRequest
	for _, opt := range opts {
		if err := opt(&purgeReq); err != nil {
			return err
		}
	}
	var req []byte
	var err error
	req, err = json.Marshal(purgeReq)
	if err != nil {
		return err
	}

	purgeSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamPurgeT, s.name))

	var resp streamPurgeResponse
	if _, err = s.jetStream.apiRequestJSON(ctx, purgeSubject, &resp, req); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}

	return nil
}

func validateDurableName(dur string) error {
	if strings.Contains(dur, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidDurableName, dur)
	}
	return nil
}

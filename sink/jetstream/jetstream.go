package jetstream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"github.com/timsolov/psql-streamer/common"
	"github.com/timsolov/psql-streamer/event"
)

// stub: s *JetStreamSink sink.Sink
// JetStreamSink ...
type JetStreamSink struct {
	name string

	servers      []string
	conn         *nats.Conn
	js           nats.JetStreamContext
	stream       *nats.StreamInfo
	writeTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	promTags []string
	stats    struct {
		total, errors, noSubject, skipped, messages uint64
	}
	*common.Logger
}

// New creates a new JetStream sink
func New(name string, v *viper.Viper) (s *JetStreamSink, err error) {
	s = &JetStreamSink{
		name:         name,
		servers:      v.GetStringSlice("servers"),
		writeTimeout: v.GetDuration("timeout"),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	var opts []nats.Option
	switch v.GetString("auth") {
	case "token":
		opts = append(opts, nats.Token(v.GetString("token")))
	}

	s.Logger = common.LoggerCreate(s, v)
	s.promTags = []string{s.Name(), s.Type()}

	opts = append(opts,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(time.Second),
	)

	// Create the NATS connection.
	if s.conn, err = nats.Connect(strings.Join(s.servers, ","), opts...); err != nil {
		return nil, fmt.Errorf("could not create NATS connection: %w", err)
	}

	// Create Jetstream context.
	if s.js, err = s.conn.JetStream(); err != nil {
		return nil, fmt.Errorf("could not create Jetstream context: %w", err)
	}

	return s, nil
}

func (s *JetStreamSink) ProcessEventsBatch(events []event.Event) error {
	atomic.AddUint64(&s.stats.total, uint64(len(events)))

	start := time.Now()
	msgCount := 0
	var (
		id      int64
		subject string
		payload string
		ok      bool
	)
	for _, event := range events {
		msgCount++

		if id, ok = event.Columns["id"].(int64); !ok {
			return fmt.Errorf("id column: absent")
		}
		if subject, ok = event.Columns["subject"].(string); !ok {
			return fmt.Errorf("subject column: absent")
		}
		if payload, ok = event.Columns["payload"].(string); !ok {
			return fmt.Errorf("payload column: absent")
		}

		ctx, cancel := context.WithTimeout(s.ctx, s.writeTimeout)

		_, err := s.js.Publish(subject, []byte(payload),
			nats.MsgId(strconv.FormatInt(id, 10)),
			nats.Context(ctx),
		)

		cancel()

		if err != nil {
			return fmt.Errorf("publish to JetStream: %w", err)
		}

		s.LogVerboseEv(event.UUID, "Event (%+v)", event)

		if event.Ack != nil {
			event.Ack()
		}
	}

	s.Debugf("(%d messages) successfully written in %.4f sec", msgCount, time.Since(start).Seconds())
	atomic.AddUint64(&s.stats.messages, uint64(msgCount))

	return nil
}

func (s *JetStreamSink) Name() string {
	return s.name
}

func (s *JetStreamSink) Type() string {
	return "Sink-JetStream"
}

// SetLogger sets a logger
func (s *JetStreamSink) SetLogger(l *common.Logger) {
	s.Logger = l
}

func (s *JetStreamSink) Stats() string {
	t := fmt.Sprintf("total: %d, no subject: %d, skipped: %d, errors: %d, jetstream msgs sent: %d",
		atomic.LoadUint64(&s.stats.total),
		atomic.LoadUint64(&s.stats.noSubject),
		atomic.LoadUint64(&s.stats.skipped),
		atomic.LoadUint64(&s.stats.errors),
		atomic.LoadUint64(&s.stats.messages),
	)

	return t
}

func (s *JetStreamSink) Status() error {
	return nil
}

func (s *JetStreamSink) Close() error {
	s.cancel()
	s.conn.Close()
	return nil
}

package jetstream

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
)

// stub: s *JetStreamSink sink.Sink
// JetStreamSink ...
type JetStreamSink struct {
	name string

	servers    []string
	streamName string
	subjects   []string
	conn       *nats.Conn
	js         nats.JetStreamContext
	stream     *nats.StreamInfo

	promTags []string
	stats    struct {
		total, errors, noSubject, skipped, messages uint64
	}
	*common.Logger
}

// New creates a new JetStream sink
func New(name string, v *viper.Viper) (s *JetStreamSink, err error) {
	s = &JetStreamSink{
		name:       name,
		servers:    v.GetStringSlice("servers"),
		streamName: v.GetString("streamName"),
		subjects:   v.GetStringSlice("subjects"),
	}

	var opts []nats.Option
	switch v.GetString("auth") {
	case "token":
		opts = append(opts, nats.Token(v.GetString("token")))
	}

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

	if s.stream, err = s.js.StreamInfo(s.streamName); err != nil {
		// Create the stream, which stores messages received on the subject.
		cfg := &nats.StreamConfig{
			Name:     s.streamName,
			Subjects: s.subjects,
			Storage:  nats.FileStorage,
		}

		if s.stream, err = s.js.AddStream(cfg); err != nil {
			return nil, fmt.Errorf("could not create NATS stream: %w", err)
		}
	}

	s.Logger = common.LoggerCreate(s, v)
	s.promTags = []string{s.Name(), s.Type()}

	return s, nil
}

func (s *JetStreamSink) ProcessEventsBatch(events []event.Event) error {
	atomic.AddUint64(&s.stats.total, uint64(len(events)))

	start := time.Now()
	msgCount := 0
	for _, event := range events {
		msgCount++
		s.js.PublishAsync("foo", []byte("OK"))
		s.LogVerboseEv(event.UUID, "Event (%+v)", event)
	}
	<-s.js.PublishAsyncComplete()

	s.Debugf("(%d messages) successfully written in %.4f sec", msgCount, time.Since(start).Seconds())
	atomic.AddUint64(&s.stats.messages, uint64(msgCount))

	return nil
}

func (s *JetStreamSink) Name() string {
	return s.name
}

func (s *JetStreamSink) Type() string {
	return "jetstream"
}

// SetLogger sets a logger
func (s *JetStreamSink) SetLogger(l *common.Logger) {
	s.Logger = l
}

func (s *JetStreamSink) Stats() string {
	t := fmt.Sprintf("total: %d, no subject: %d, skipped: %d, errors: %d, jetstream msgs sent: %d, handlers:",
		atomic.LoadUint64(&s.stats.total),
		atomic.LoadUint64(&s.stats.noSubject),
		atomic.LoadUint64(&s.stats.skipped),
		atomic.LoadUint64(&s.stats.errors),
		atomic.LoadUint64(&s.stats.messages),
	)

	// for k, v := range k.eventHandlers {
	// 	t += fmt.Sprintf(" [%s: %s]", k, v.Stats())
	// }

	return t
}

func (s *JetStreamSink) Status() error {
	return nil
}

func (s *JetStreamSink) Close() error {
	s.conn.Close()
	return nil
}

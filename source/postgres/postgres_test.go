package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/timsolov/psql-streamer/mux"

	"github.com/timsolov/pgoutput"
	"github.com/timsolov/psql-streamer/common"
	"github.com/timsolov/psql-streamer/event"
	"github.com/timsolov/psql-streamer/sink"
	"github.com/timsolov/psql-streamer/sink/stub"
)

var (
	rel = &pgoutput.Relation{ID: 0x2d7cb, Namespace: "public", Name: "test", Replica: 0x64, Columns: []pgoutput.Column{{Key: true, Name: "a", Type: 0x17, Mode: 0xffffffff}, {Key: false, Name: "b", Type: 0x19, Mode: 0xffffffff}}}
	ins = &pgoutput.Insert{RelationID: 0x2d7cb, New: true, Row: []pgoutput.Tuple{{Flag: 116, Value: []uint8{0x34, 0x30, 0x30, 0x31, 0x36, 0x36}}, {Flag: 116, Value: []uint8{0x61}}}}
	upd = &pgoutput.Update{RelationID: 0x2d7cb, Old: false, Key: false, New: true, OldRow: []pgoutput.Tuple(nil), Row: []pgoutput.Tuple{{Flag: 116, Value: []uint8{0x31, 0x34, 0x34, 0x31, 0x31, 0x30}}, {Flag: 116, Value: []uint8{0x6c, 0x61, 0x6c, 0x61}}}}
	del = &pgoutput.Delete{RelationID: 0x2d7cb, Key: true, Old: false, Row: []pgoutput.Tuple{{Flag: 116, Value: []uint8{0x31, 0x34, 0x34, 0x31, 0x31, 0x30}}, {Flag: 0, Value: []uint8(nil)}}}
	beg = &pgoutput.Begin{LSN: 0x192d1188, XID: 102693}
)

func TestPSQL(t *testing.T) {
	s := &PSQL{
		name: "Test",
		cfg: psqlConfig{
			startRetryInterval: 1 * time.Microsecond,
		},

		relationSet: pgoutput.NewRelationSet(nil),
		promTags:    []string{"test", "psql"},
		sinks:       map[string]sink.Sink{},
		connConfig:  &pgconn.Config{},
	}

	s.SetLogger(common.LoggerCreate(s, nil))
	s.mux, _ = mux.New(context.Background(), nil, mux.Config{
		Logger: s.Logger,
	})

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.sinks["test"], _ = stub.New("test", nil,
		func(e []event.Event) error {
			assert.NotNil(t, e)
			return nil
		},
	)

	ins.SetWalStart(1)
	ins.SetWalEnd(1)
	err := s.process([]pgoutput.Message{ins})
	assert.NotNil(t, err) // Relation with ID '186315' not found in relationSet

	rel.SetWalStart(2)
	rel.SetWalEnd(2)
	err = s.process([]pgoutput.Message{rel})
	assert.Nil(t, err)

	upd.SetWalStart(3)
	upd.SetWalEnd(3)
	err = s.process([]pgoutput.Message{upd})
	assert.Nil(t, err)

	del.SetWalStart(4)
	del.SetWalEnd(4)
	err = s.process([]pgoutput.Message{del})
	assert.Nil(t, err)

	stats := s.Stats()
	assert.Contains(t, stats, "events: 2, eventErrors: 1, replicaErrors: 0, persistErrors: 0, walPos: 0, walPosPersist: 0")

	retry := 0
	s.sinks["test"], _ = stub.New("test", nil,
		func(e []event.Event) error {
			assert.NotNil(t, e)
			if retry < 2 {
				retry++
				return fmt.Errorf("Foo")
			}

			return nil
		})

	err = s.process([]pgoutput.Message{ins})
	assert.Nil(t, err)

	assert.Equal(t, "Source-Postgres2", s.Type())
	assert.Equal(t, "Test", s.Name())

	err = s.process([]pgoutput.Message{beg})
	assert.Nil(t, err)

	assert.Nil(t, s.Status())
	s.setError(fmt.Errorf("foo"))
	assert.NotNil(t, s.Status())

	// Generate event test
	_, err = s.generateEvent(event.ActionInsert, 666, ins.Row)
	assert.NotNil(t, err)

	ev, err := s.generateEvent(event.ActionInsert, 0x2d7cb, ins.Row)
	assert.Nil(t, err)
	assert.Equal(t, "test", ev.Table)
	assert.Equal(t, "", ev.Database)
	assert.Equal(t, event.ActionInsert, ev.Action)
	assert.Equal(t, uint64(0), ev.WALEndPosition)
	assert.Equal(t, 2, len(ev.Columns))
	assert.Equal(t, int32(400166), ev.Columns["a"].(int32))
	assert.Equal(t, "a", ev.Columns["b"].(string))
}

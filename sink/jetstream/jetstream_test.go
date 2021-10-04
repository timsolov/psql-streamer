package jetstream

import (
	"testing"
	"time"

	"github.com/blind-oracle/psql-streamer/event"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func Test_JetStream(t *testing.T) {
	ev := event.Event{
		Host:     "foo",
		Database: "bar",
		Table:    "baz",
		Action:   "insert",
		Columns: map[string]interface{}{
			"id":      int64(1),
			"subject": "test.subject",
			"payload": `{"msg":"test message"}`,
		},
		Timestamp: time.Unix(123456789, 0),
	}

	v := viper.New()
	v.Set("servers", []string{"0.0.0.0:14222"})
	v.Set("auth", "token")
	v.Set("token", "test")
	v.Set("timeout", time.Second)

	jetstream, err := New("jetstream", v)
	assert.Nil(t, err)

	err = jetstream.ProcessEventsBatch([]event.Event{ev})
	assert.Nil(t, err)

	err = jetstream.Close()
	assert.Nil(t, err)
}

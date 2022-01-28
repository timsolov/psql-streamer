package kafkasink

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/timsolov/psql-streamer/event"
)

func Test_Kafka(t *testing.T) {
	ev := event.Event{
		Host:     "foo",
		Database: "bar",
		Table:    "baz",
		Action:   "insert",
		Columns: map[string]interface{}{
			"a": "B",
		},
		Timestamp: time.Unix(123456789, 0),
	}

	v := viper.New()
	v.Set("hosts", []string{"127.0.0.1:1234"})
	v.Set("handlers", []string{"passthrough"})
	v.Set("topicFallback", "tfb")
	v.Set("async", true)

	kfk, err := New("test", v)
	assert.Nil(t, err)

	err = kfk.ProcessEventsBatch([]event.Event{ev})
	assert.Nil(t, err)

	err = kfk.Close()
	assert.Nil(t, err)
}

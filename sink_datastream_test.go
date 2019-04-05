package flinkcodegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSinkDataStream(t *testing.T) {
	ds := NewDataStream("left", "topic")
	ssd := NewSinkDataStream("output", "localhost:9092", ds)

	assert.NotNil(t, ssd)
	assert.EqualValues(t, "output", ssd.Topic)
	assert.EqualValues(t, "localhost:9092", ssd.BrokerURL)
	assert.EqualValues(t, ds.Name, ssd.LeftStream.Name)
}

func TestSinkStreamRender(t *testing.T) {
	ds := NewDataStream("left", "topic")
	ssd := NewSinkDataStream("output", "localhost:9092", ds)

	r := ssd.Render()
	jo := `left.addSink(new FlinkKafkaProducer011<>("localhost:9092","output", new SimpleStringSchema()));`

	assert.EqualValues(t, jo, r)
}

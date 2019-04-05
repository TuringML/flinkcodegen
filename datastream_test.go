package flinkcodegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDataStream(t *testing.T) {
	ds := NewDataStream("test", "topic")

	assert.NotNil(t, ds)
	assert.EqualValues(t, "test", ds.Name)
	assert.EqualValues(t, "topic", ds.Topic)
}

func TestDataStreamRender(t *testing.T) {
	ds := NewDataStream("test", "topic")
	r := ds.Render()

	jo := `DataStream<String> test = 
		env.addSource(new FlinkKafkaConsumer011<>("topic", new AvroDeserializationSchema(), parameterTool.getProperties()));`

	assert.EqualValues(t, jo, r)
}

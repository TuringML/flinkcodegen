package flinkcodegen

import "github.com/hoisie/mustache"

// SinkDataStream represents the basic unit for publishing information to Kafka
type SinkDataStream struct {
	Topic      string
	BrokerURL  string
	LeftStream *DataStream
}

// NewSinkDataStream creates a new DataStream object used for then rendering the Java Flink DataStream object
func NewSinkDataStream(topic, brokersURL string, leftStream *DataStream) *SinkDataStream {
	return &SinkDataStream{
		Topic:      topic,
		BrokerURL:  brokersURL,
		LeftStream: leftStream,
	}
}

// Render will generate the string of the current DataStream object
func (sds *SinkDataStream) Render() string {
	javaObjStr := `{{ name }}.addSink(new FlinkKafkaProducer011<>("{{ brokersURL }}","{{ topic }}", new AvroSerializationSchema()));`

	return mustache.Render(javaObjStr, map[string]string{
		"name":       sds.LeftStream.Name,
		"brokersURL": sds.BrokerURL,
		"topic":      sds.Topic,
	})
}

package flinkcodegen

import "github.com/hoisie/mustache"

// DataStream represents the basic unit for publishing/consuming information from Kafka
type DataStream struct {
	Name  string
	Topic string
}

// NewDataStream creates a new DataStream object used for then rendering the Java Flink DataStream object
func NewDataStream(name, topic string) *DataStream {
	return &DataStream{
		Name:  name,
		Topic: topic,
	}
}

// Render will generate the string of the current DataStream object
func (ds *DataStream) Render() string {
	javaObjStr := `DataStream<String> {{ name }} = env.addSource(
		new FlinkKafkaConsumer011<>({{ topic }}, new AvroDeserializationSchema(), parameterTool.getProperties())
	);`

	return mustache.Render(javaObjStr, map[string]string{"name": ds.Name, "topic": ds.Topic})
}

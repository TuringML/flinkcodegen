package flinkcodegen

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProject(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	assert.NotNil(t, p)
}

func TestSourceStream(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SourceStream("right", "topic-right", false)

	assert.NotNil(t, p.LeftStream)
	assert.NotNil(t, p.RightStream)
}

func TestInitExtraStream(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")

	p.InitExtraStreams([]*NameTopic{
		&NameTopic{
			Name:  "first",
			Topic: "topic1",
		},
		&NameTopic{
			Name:  "second",
			Topic: "topic2",
		},
	})

	assert.EqualValues(t, 2, len(p.ExtraStreams))
}

func TestSinkStream(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SinkStream()

	assert.NotNil(t, p.LeftStream)
	assert.NotNil(t, p.SinkDataStream)
}

func TestRenderWindowJoin(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SourceStream("right", "topic-right", false)

	j, err := p.RenderWindowJoin("join", "left", "right", 20)
	if err != nil {
		t.Error(err)
	}

	jo := `DataStream<String> join = left.join(right)
	.where("left").equalTo("right")
	.window(TumblingEventTimeWindows.of(Time.seconds(20)));`

	assert.EqualValues(t, jo, j)
}

func TestFailRenderWindowJoin(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)

	_, err := p.RenderWindowJoin("join", "left", "right", 20)
	assert.NotNil(t, err)
}

func TestRenderUnion(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SourceStream("right", "topic-right", false)

	p.InitExtraStreams([]*NameTopic{
		&NameTopic{
			Name:  "first",
			Topic: "topic1",
		},
		&NameTopic{
			Name:  "second",
			Topic: "topic2",
		},
	})

	j, err := p.RenderUnion("union")
	if err != nil {
		t.Error(err)
	}

	jo := `DataStream<String> union = left.union(first,second,right)`

	assert.EqualValues(t, jo, j)
}

func TestFailRenderUnion(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("right", "topic-right", false)

	_, err := p.RenderUnion("join")
	assert.NotNil(t, err)
}

func TestRenderAllStream(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SourceStream("right", "topic-right", false)

	streams, err := p.RenderSourceStreams()
	if err != nil {
		t.Error(err)
	}

	jo := `DataStream<String> left = 
		env.addSource(new FlinkKafkaConsumer011<>("topic-left", new AvroDeserializationSchema(), parameterTool.getProperties()));
DataStream<String> right = 
		env.addSource(new FlinkKafkaConsumer011<>("topic-right", new AvroDeserializationSchema(), parameterTool.getProperties()));`

	assert.EqualValues(t, jo, streams)
}

func TestRenderAllStreamExtraStreams(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)

	p.InitExtraStreams([]*NameTopic{
		&NameTopic{
			Name:  "first",
			Topic: "topic1",
		},
		&NameTopic{
			Name:  "second",
			Topic: "topic2",
		},
	})

	streams, err := p.RenderSourceStreams()
	if err != nil {
		t.Error(err)
	}

	jo := `DataStream<String> left = 
		env.addSource(new FlinkKafkaConsumer011<>("topic-left", new AvroDeserializationSchema(), parameterTool.getProperties()));
DataStream<String> first = 
		env.addSource(new FlinkKafkaConsumer011<>("topic1", new AvroDeserializationSchema(), parameterTool.getProperties()));
DataStream<String> second = 
		env.addSource(new FlinkKafkaConsumer011<>("topic2", new AvroDeserializationSchema(), parameterTool.getProperties()));`

	assert.EqualValues(t, jo, streams)
}

func TestGenerateProject(t *testing.T) {
	p := NewProject("name", "output", "topic-out", "localhost:9092", "test-group")
	p.SourceStream("left", "topic-left", true)
	p.SourceStream("right", "topic-right", false)
	p.SinkStream()

	streams, err := p.RenderSourceStreams()
	if err != nil {
		t.Error(err)
	}

	sinkStream, err := p.RenderSinkStream()
	if err != nil {
		t.Error(err)
	}

	joinOperation, err := p.RenderWindowJoin("join", "left", "right", 20)
	if err != nil {
		t.Error(err)
	}

	if err := p.GenerateProject(streams, joinOperation, sinkStream); err != nil {
		t.Error(err)
	}

	// check if files exist
	if _, err := os.Stat(path.Join(p.OutputPath, "/java/src/com/turingml/Main.java")); os.IsNotExist(err) {
		t.Error(err)
	}

	if _, err := os.Stat(path.Join(p.OutputPath, "/resources/log4j.properties")); os.IsNotExist(err) {
		t.Error(err)
	}

	if _, err := os.Stat(path.Join(p.OutputPath, "pom.xml")); os.IsNotExist(err) {
		t.Error(err)
	}

	// delete output afterwards
	if err = os.RemoveAll(p.OutputPath); err != nil {
		t.Error(err)
	}
}

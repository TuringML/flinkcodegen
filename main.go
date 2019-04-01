package main

import (
	"github.com/turing-ml/flink-codegen/flinkcodegen"
)

func main() {
	// create new project
	p := flinkcodegen.NewProject("test", "output", "topic-outpit")

	// init datastreams and get the rendered value
	p.SourceStream("leftDS", "test-join", true)
	p.SourceStream("rightDS", "test-join-new", false)
	p.SinkStream("localhost:9092")

	// render every source stream created above
	sourceStreams := p.RenderSourceStreams()
	sinkStream := p.RenderSinkStream()

	// render the operation you want to perform
	join, err := p.RenderWindowJoin("joinDS", "name-left", "name-right", 5)
	if err != nil {
		panic(err)
	}

	// Generate files of the project
	err = p.GenerateProject(sourceStreams, join)
	if err != nil {
		panic(err)
	}
}

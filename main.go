package main

import (
	"github.com/turing-ml/flink-codegen/flinkcodegen"
)

func main() {
	// create new project
	p := flinkcodegen.NewProject("test", "output")

	// init datastreams and get the rendered value
	p.InitStream("leftDS", "test-join", true)
	p.InitStream("rightDS", "test-join-new", false)

	// render every stream created above
	streams := p.RenderAllStreams()

	// render the operation you want to perform
	join, err := p.RenderWindowJoin("joinDS", "name-left", "name-right", 5)
	if err != nil {
		panic(err)
	}

	// Generate files of the project
	err = p.GenerateProject(streams, join)
	if err != nil {
		panic(err)
	}
}

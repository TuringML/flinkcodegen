# Flink Code Generation

This is a simple library that will help in generating some flink operations.

## DataStreams

The datastreams will read only from a valid Kafka Topic. 

## How to use it

```golang
package main

import (
    "github.com/turing-ml/flinkcodegen"
)

func main() {
    // create new project
    p := flinkcodegen.NewProject("test", "output", "topic-outpit", "localhost:9092", "group-test")

    // init datastreams and get the rendered value
    p.SourceStream("leftDS", "test-join", true)
    p.SourceStream("rightDS", "test-join-new", false)
    p.SinkStream()

    // render every source stream created above
    sourceStreams, _ := p.RenderSourceStreams()
    sinkStream, _ := p.RenderSinkStream()

    // render the operation you want to perform
    join, err := p.RenderWindowJoin("joinDS", "name-left", "name-right", 5)
    if err != nil {
        panic(err)
    }

    // Generate files of the project
    err = p.GenerateProject(sourceStreams, join, sinkStream)
    if err != nil {
        panic(err)
    }
}
```

# TODO

- [ ] Avro Serialization when sinking via Kafka (not sure if possible)
- [x] Add more tests
- [ ] Add Docker image (?)
- [ ] Add Maven pipeline for testing the output
- [ ] Add CI/CD (?)

package flinkcodegen

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/hoisie/mustache"
)

// Project is the main container before generating the Java code
type Project struct {
	Name           string
	OutputPath     string
	OutputTopic    string
	BrokersURL     string
	GroupID        string
	LeftStream     *DataStream
	RightStream    *DataStream
	ExtraStreams   []*DataStream
	SinkDataStream *SinkDataStream
}

// NewProject will return the skeleton for a Java project
func NewProject(name, outputPath, outputTopic, borkersURL, groupID string) *Project {
	pwd, _ := os.Getwd()
	fmt.Println(pwd)
	return &Project{
		Name:        name,
		BrokersURL:  borkersURL,
		GroupID:     groupID,
		OutputTopic: outputTopic,
		OutputPath:  path.Join(pwd, outputPath),
	}
}

// SourceStream will add the datastream that the operation will be perfomed on
func (p *Project) SourceStream(name, topic string, isLeft bool) {
	if isLeft {
		p.LeftStream = NewDataStream(name, topic)
	} else {
		p.RightStream = NewDataStream(name, topic)
	}
}

// SinkStream will generate a datastream where to store the results
func (p *Project) SinkStream() {
	p.SinkDataStream = &SinkDataStream{
		Topic:      p.OutputTopic,
		BrokerURL:  p.BrokersURL,
		LeftStream: p.LeftStream,
	}
}

// NameTopic is a support struct for creating multiple datastreams
type NameTopic struct {
	Name  string
	Topic string
}

// InitExtraStreams is used in particular with Union when multiple streams will be union with
func (p *Project) InitExtraStreams(nameTopic []*NameTopic) {
	for _, nt := range nameTopic {
		p.ExtraStreams = append(p.ExtraStreams, NewDataStream(nt.Name, nt.Topic))
	}
}

// GenerateProject will render the Java code
func (p *Project) GenerateProject(renderedStreams, renderedOperation, renderedSinkStream string) error {
	mainPath := path.Join(p.OutputPath, "/java/src/com/turingml")

	pwd, _ := os.Getwd()
	m := mustache.RenderFile(path.Join(pwd, "/resources/Main.java"), map[string]string{
		"broker_servers": p.BrokersURL,
		"group_id":       p.GroupID,
		"source_streams": renderedStreams,
		"functions":      renderedOperation,
		"sink_stream":    renderedSinkStream,
	})

	// write the Main.java to output
	err := writeToFile(mainPath, "Main.java", m)
	if err != nil {
		return err
	}

	// copy log4j.properties to output path
	resourcesPath := path.Join(p.OutputPath, "/resources")
	err = copyFile(path.Join(pwd, "/resources/log4j.properties"), resourcesPath, "log4j.properties")
	if err != nil {
		return err
	}

	// copy pom.xml to output path
	return copyFile(path.Join(pwd, "/resources/pom.xml"), p.OutputPath, "pom.xml")
}

// RenderWindowJoin will render the window join operation
func (p *Project) RenderWindowJoin(name, leftAttribute, rightAttribute string, windowSize int) (string, error) {
	if p.LeftStream == nil || p.RightStream == nil {
		return "", errors.New("either left or right stream has not been initialized")
	}

	wj := NewWindowJoin(name, leftAttribute, rightAttribute, windowSize, p.LeftStream, p.RightStream)
	return wj.Render(), nil
}

// RenderUnion will render the union operation
func (p *Project) RenderUnion(name string) (string, error) {
	if p.LeftStream == nil || p.RightStream == nil {
		return "", errors.New("either left or right stream has not been initialized")
	}

	u := NewUnion(name, p.LeftStream, append(p.ExtraStreams, p.RightStream))
	return u.Render(), nil
}

// RenderSourceStreams will return the rendered string with all the streams
func (p *Project) RenderSourceStreams() (string, error) {
	streams := []string{}

	if p.LeftStream == nil {
		return "", errors.New("left stream has not been initialized")
	}
	streams = append(streams, p.LeftStream.Render())

	if p.RightStream != nil {
		streams = append(streams, p.RightStream.Render())
	}

	for _, st := range p.ExtraStreams {
		streams = append(streams, st.Render())
	}
	return strings.Join(streams, "\n"), nil
}

// RenderSinkStream will render the sink stream
func (p *Project) RenderSinkStream() (string, error) {
	if p.SinkDataStream == nil {
		return "", errors.New("sink stream has not been initialized")
	}
	return p.SinkDataStream.Render(), nil
}

func writeToFile(p, fileName, s string) error {
	// Create path if not exists
	if _, err := os.Stat(p); os.IsNotExist(err) {
		if err = os.MkdirAll(p, os.ModePerm); err != nil {
			return err
		}
	}

	// Create file
	f, err := os.Create(path.Join(p, fileName))
	if err != nil {
		return err
	}
	defer f.Close()

	// write content to file
	_, err = f.WriteString(s)
	if err != nil {
		return err
	}

	return nil
}

func copyFile(src, dst, fileName string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err = os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	out, err := os.Create(path.Join(dst, fileName))
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

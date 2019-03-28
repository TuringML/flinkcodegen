package flinkcodegen

import (
	"errors"
	"io"
	"os"
	"strings"

	"github.com/hoisie/mustache"
)

// OperationType is the enum of the type of operations that needs to be performed
type OperationType int

const (
	// UnionType will perform an union among datastreams
	UnionType OperationType = iota
	// WindowJoinType will perform a join between two datastreams
	WindowJoinType
)

// Project is the main container before generating the Java code
type Project struct {
	Name          string
	OutputPath    string
	LeftStream    *DataStream
	RightStream   *DataStream
	ExtraStreams  []*DataStream
	OperationType OperationType
}

// NewProject will return the skeleton for a Java project
func NewProject(name, outputPath string, t OperationType) *Project {
	return &Project{
		Name:          name,
		OutputPath:    outputPath,
		OperationType: t,
	}
}

// InitLeftStream will add the datastream that the operation will be perfomed on
func (p *Project) InitLeftStream(name, topic string) {
	p.LeftStream = NewDataStream(name, topic)
}

// InitRightStream will add the datastream that will be union,joined, etc with
func (p *Project) InitRightStream(name, topic string) {
	p.RightStream = NewDataStream(name, topic)
}

// InitExtraStreams is used in particular with Union when multiple streams will be union with
func (p *Project) InitExtraStreams(nameTopic map[string]string) {
	for name, topic := range nameTopic {
		p.ExtraStreams = append(p.ExtraStreams, NewDataStream(name, topic))
	}
}

// GenerateProject will render the Java code
func (p *Project) GenerateProject(renderedOperation string) error {

	path := strings.TrimSuffix(p.OutputPath, "/") + "/java/src/com/turingml/Main.java"
	m := mustache.RenderFile("./resources/Main.java", map[string]string{
		"functions": renderedOperation,
	})

	// write the Main.java to output
	err := writeToFile(path, m)
	if err != nil {
		return err
	}

	// copy pom.xml to output path
	po := strings.TrimSuffix(p.OutputPath, "/") + "/pom.xml"
	copyFile("./resources/pom.xml", po)

	return nil
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

func writeToFile(path, s string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(s)
	if err != nil {
		return err
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
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

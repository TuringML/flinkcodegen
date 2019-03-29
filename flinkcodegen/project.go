package flinkcodegen

import (
	"errors"
	"io"
	"os"
	"path"
	"strings"

	"github.com/hoisie/mustache"
)

// Project is the main container before generating the Java code
type Project struct {
	Name         string
	OutputPath   string
	LeftStream   *DataStream
	RightStream  *DataStream
	ExtraStreams []*DataStream
}

// NewProject will return the skeleton for a Java project
func NewProject(name, outputPath string) *Project {
	pwd, _ := os.Getwd()
	return &Project{
		Name:       name,
		OutputPath: path.Join(pwd, outputPath),
	}
}

// InitStream will add the datastream that the operation will be perfomed on
func (p *Project) InitStream(name, topic string, isLeft bool) {
	if isLeft {
		p.LeftStream = NewDataStream(name, topic)
	} else {
		p.RightStream = NewDataStream(name, topic)
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
func (p *Project) GenerateProject(renderedStreams, renderedOperation string) error {
	mainPath := path.Join(p.OutputPath, "/java/src/com/turingml")

	pwd, _ := os.Getwd()
	m := mustache.RenderFile(path.Join(pwd, "/resources/Main.java"), map[string]string{
		"streams":   renderedStreams,
		"functions": renderedOperation,
	})

	// write the Main.java to output
	err := writeToFile(mainPath, "Main.java", m)
	if err != nil {
		return err
	}

	// copy pom.xml to output path
	po := path.Join(p.OutputPath, "pom.xml")
	return copyFile(path.Join(pwd, "/resources/pom.xml"), po)
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

// RenderAllStreams will return the rendered string with all the streams
func (p *Project) RenderAllStreams() string {
	streams := []string{}

	streams = append(streams, p.LeftStream.Render())
	streams = append(streams, p.RightStream.Render())

	for _, st := range p.ExtraStreams {
		streams = append(streams, st.Name)
	}
	return strings.Join(streams, "\n")
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

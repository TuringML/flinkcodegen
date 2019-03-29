package flinkcodegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProject(t *testing.T) {
	p := NewProject("name", "output")
	assert.NotNil(t, p)
}

func TestInitStream(t *testing.T) {
	p := NewProject("name", "output")
	p.InitStream("left", "topic-left", true)
	p.InitStream("right", "topic-right", false)

	assert.NotNil(t, p.LeftStream)
	assert.NotNil(t, p.RightStream)
}

func TestInitExtraStream(t *testing.T) {
	p := NewProject("name", "output")

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

func TestRenderWindowJoin(t *testing.T) {

}

func TestRenderUnion(t *testing.T) {

}

func TestRenderAllStream(t *testing.T) {

}

func TestGenerateProject(t *testing.T) {

}

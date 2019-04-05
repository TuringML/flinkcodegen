package flinkcodegen

import (
	"strings"

	"github.com/hoisie/mustache"
)

// Union is the object that will union to the Stream, multiple streams
type Union struct {
	Name    string
	Stream  *DataStream
	Streams []*DataStream
}

// NewUnion creates a new union object
func NewUnion(n string, s *DataStream, ss []*DataStream) *Union {
	return &Union{
		Name:    n,
		Stream:  s,
		Streams: ss,
	}
}

// Render will generate the string of the current WindowJoin object
func (u *Union) Render() string {
	javaObjStr := `DataStream<String> {{ name }} = {{ stream }}.union({{ streams }})`

	streams := []string{}
	for _, st := range u.Streams {
		streams = append(streams, st.Name)
	}

	return mustache.Render(javaObjStr, map[string]string{
		"name":    u.Name,
		"stream":  u.Stream.Name,
		"streams": strings.Join(streams, ","),
	})
}

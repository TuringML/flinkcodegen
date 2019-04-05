package flinkcodegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUnion(t *testing.T) {
	ls := NewDataStream("left", "left-topic")

	var ss []*DataStream
	rs := NewDataStream("right", "right-topic")
	es := NewDataStream("extra", "right-topic")

	wj := NewUnion("name", ls, append(ss, rs, es))

	assert.NotNil(t, wj)
	assert.EqualValues(t, "name", wj.Name)
	assert.EqualValues(t, 2, len(wj.Streams))
}

func TestUnionRender(t *testing.T) {
	ls := NewDataStream("left", "left-topic")

	var ss []*DataStream
	rs := NewDataStream("right", "right-topic")
	es := NewDataStream("extra", "right-topic")

	wj := NewUnion("name", ls, append(ss, rs, es))
	r := wj.Render()

	jo := `DataStream<String> name = left.union(right,extra)`

	assert.EqualValues(t, jo, r)
}

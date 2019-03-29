package flinkcodegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWindowJoin(t *testing.T) {
	ls := NewDataStream("left", "left-topic")
	rs := NewDataStream("right", "right-topic")

	wj := NewWindowJoin("name", "left", "right", 5, ls, rs)

	assert.NotNil(t, wj)
	assert.EqualValues(t, "name", wj.Name)
	assert.EqualValues(t, "left", wj.LeftAttribute)
	assert.EqualValues(t, "right", wj.RightAttribute)
	assert.EqualValues(t, 5, wj.WindowSizeInSeconds)
	assert.NotNil(t, ls, wj.LeftStream)
	assert.NotNil(t, rs, wj.RightStream)
}

func TestWindowJoinRender(t *testing.T) {

	ls := NewDataStream("left", "left-topic")
	rs := NewDataStream("right", "right-topic")

	wj := NewWindowJoin("name", "left", "right", 5, ls, rs)
	r := wj.Render()

	jo := `DataStream<String> name = left.join(right)
	.where("left").equalTo("right")
	.window(TumblingEventTimeWindows.of(Time.seconds(5)));`

	assert.EqualValues(t, jo, r)
}

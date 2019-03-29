package flinkcodegen

import "github.com/hoisie/mustache"

// WindowJoin is the object that will contain the two datastreams that need to be joined together
type WindowJoin struct {
	Name                string
	LeftAttribute       string
	RightAttribute      string
	WindowSizeInSeconds int
	LeftStream          *DataStream
	RightStream         *DataStream
}

// NewWindowJoin returns a new WindowJoin object used for then rendering the Java Flink DataStream object
func NewWindowJoin(name, la, ra string, ws int, ls, rs *DataStream) *WindowJoin {
	return &WindowJoin{
		Name:                name,
		LeftAttribute:       la,
		RightAttribute:      ra,
		WindowSizeInSeconds: ws,
		LeftStream:          ls,
		RightStream:         rs,
	}
}

// Render will generate the string of the current WindowJoin object
func (wj *WindowJoin) Render() string {
	javaObjStr := `DataStream<String> {{ name }} = {{ leftStreamName }}.join({{ rightStreamName }})
	.where("{{ leftAttribute }}").equalTo("{{ rightAttribute }}")
	.window(TumblingEventTimeWindows.of(Time.seconds({{ windowSize }})));`

	return mustache.Render(javaObjStr, map[string]interface{}{
		"name":            wj.Name,
		"leftStreamName":  wj.LeftStream.Name,
		"rightStreamName": wj.RightStream.Name,
		"leftAttribute":   wj.LeftAttribute,
		"rightAttribute":  wj.RightAttribute,
		"windowSize":      wj.WindowSizeInSeconds,
	})
}

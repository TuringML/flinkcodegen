package flinkcodegen

// WindowJoin is the object that will contain the two datastreams that need to be joined together
type WindowJoin struct {
	Name        string
	LeftStream  *DataStream
	RightStream *DataStream
}

// NewWindowJoin returns a new WindowJoin object used for then rendering the Java Flink DataStream object
func NewWindowJoin(name string, ls, rs *DataStream) *WindowJoin {
	return &WindowJoin{
		Name:        name,
		LeftStream:  ls,
		RightStream: rs,
	}
}

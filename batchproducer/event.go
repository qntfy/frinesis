package batchproducer

// Event reports an async event from the batchproducer
type Event interface {
	String() string
}

var (
	_ Event = (*Error)(nil)
	_ error = (*Error)(nil)
)

// Error conforms to Event interface and reports an error from the batchproducer
type Error struct {
	str string
}

func newError(str string) *Error {
	return &Error{
		str: str,
	}
}

// String provides a string representation of the error
func (e *Error) String() string {
	return e.str
}

// Error provides a string version to conform to golang error interface
func (e *Error) Error() string {
	return e.String()
}

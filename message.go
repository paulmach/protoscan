package protoscan

import (
	"errors"
	"io"
)

//go:generate protoc --go_out=internal/testmsg internal/testmsg/types.proto
//go:generate go run internal/gen_repeated.go

// ErrIntOverflow is returned when scanning an integer with varint encoding and the
// value is too long for the integer type.
var ErrIntOverflow = errors.New("protoscan: integer overflow")

// ErrInvalidLength is returned when a length is not valid, usually resulting
// from scanning the incorrect type.
var ErrInvalidLength = errors.New("protoscan: invalid length")

// The WireType describes the encoding method for the next value in the stream.
const (
	WireTypeVarint          = 0
	WireType64bit           = 1
	WireTypeLengthDelimited = 2
	WireTypeStartGroup      = 3 // deprecated by protobuf, not supported
	WireTypeEndGroup        = 4 // deprecated by protobuf, not supported
	WireType32bit           = 5
)

// Message is a container for a protobuf message type that is ready for scanning.
type Message struct {
	data   []byte
	index  int
	length int

	err error

	fieldNumber int
	wireType    int
}

// New creates a new Message scanner for the given encoded protobuf data.
func New(data []byte) *Message {
	return &Message{
		data:   data,
		index:  0,
		length: len(data),
	}
}

// Scan will move the scanner to the next value.
func (m *Message) Scan() bool {
	if m.index < m.length {
		val, err := m.Varint64()
		if err != nil {
			m.err = err
			return false
		}
		m.fieldNumber = int(val >> 3)
		m.wireType = int(val & 0x7)
		return true
	}

	return false
}

// Err will return any errors that were encountered during scanning.
// Errors could be due to reading the incorrect types or forgetting to skip and unused value.
func (m *Message) Err() error {
	return m.err
}

// FieldNumber returns the number for the current value being scanned.
// These numbers are defined in in the protobuf definition file used to encode the message.
func (m *Message) FieldNumber() int {
	return m.fieldNumber
}

// WireType returns the 'type' of the data at the current location.
func (m *Message) WireType() int {
	return m.wireType
}

// Skip will move the scanner past the current value if it is not needed.
// If a value is not parsed this method must be called to move the decoder past the value.
func (m *Message) Skip() error {
	switch m.wireType {
	case WireTypeVarint:
		_, err := m.Varint64()
		return err
	case WireType64bit:
		if m.length <= m.index+8 {
			return io.ErrUnexpectedEOF
		}
		m.index += 8
	case WireTypeLengthDelimited:
		l, err := m.packedLength()
		if err != nil {
			return err
		}
		m.index += l
	case WireType32bit:
		if m.length <= m.index+4 {
			return io.ErrUnexpectedEOF
		}
		m.index += 4
	}

	return nil
}

// Message will return a pointer to an embedded message that can then
// be scanned in kind of a recursive fashion.
func (m *Message) Message() (*Message, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	nm := New(m.data[m.index : m.index+l])
	m.index += l
	return nm, nil
}

// MessageData returns the encoded data a message. This data can
// then be decoded using conventional tools.
func (m *Message) MessageData() ([]byte, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	return m.data[m.index : m.index+l], nil
}

func (m *Message) packedLength() (int, error) {
	l64, err := m.Varint64()
	if err != nil {
		return 0, err
	}

	l := int(l64)
	if l < 0 {
		return 0, ErrInvalidLength
	}

	postIndex := m.index + l
	if postIndex < 0 {
		// because there could be overflow...
		return 0, ErrInvalidLength
	}

	if m.length < postIndex {
		return 0, io.ErrUnexpectedEOF
	}

	return l, nil
}

func (m *Message) count(l int) int {
	var count int
	for _, b := range m.data[m.index : m.index+l] {
		if b < 128 {
			count++
		}
	}

	return count
}

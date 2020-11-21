package protoscan

import (
	"encoding/binary"
	"io"
	"math"
)

// Fixed32 reads a fixed 4 byte value as a uint32. This proto type is
// more efficient than uint32 if values are often greater than 2^28.
func (m *Message) Fixed32() (uint32, error) {
	if m.length < m.index+4 {
		return 0, io.ErrUnexpectedEOF
	}

	v := binary.LittleEndian.Uint32(m.data[m.index:])
	m.index += 4
	return v, nil
}

// Fixed64 reads a fixed 8 byte value as an uint64. This proto type is
// more efficient than uint64 if values are often greater than 2^56.
func (m *Message) Fixed64() (uint64, error) {
	if m.length < m.index+8 {
		return 0, io.ErrUnexpectedEOF
	}

	v := binary.LittleEndian.Uint64(m.data[m.index:])
	m.index += 8
	return v, nil
}

// Sfixed32 reads a fixed 4 byte value signed value.
func (m *Message) Sfixed32() (int32, error) {
	v, err := m.Fixed32()
	if err != nil {
		return 0, nil
	}

	return int32(v), nil
}

// Sfixed64 reads a fixed 8 byte signed value.
func (m *Message) Sfixed64() (int64, error) {
	v, err := m.Fixed64()
	if err != nil {
		return 0, nil
	}

	return int64(v), nil
}

// Varint32 reads up to 32-bits of variable-length encoded data.
// Note that negative int32 values could still be encoded
// as 64-bit varints due to their leading 1s.
func (m *Message) Varint32() (uint32, error) {
	var val uint32
	for shift := uint(0); ; shift += 7 {
		if shift >= 32 {
			return 0, ErrIntOverflow
		}
		if m.index >= m.length {
			return 0, io.ErrUnexpectedEOF
		}
		b := m.data[m.index]
		m.index++
		val |= uint32(b&0x7F) << shift
		if b < 0x80 {
			break
		}
	}

	return val, nil
}

// Varint64 reads up to 64-bits of variable-length encoded data.
func (m *Message) Varint64() (uint64, error) {
	var val uint64
	for shift := uint(0); ; shift += 7 {
		if shift >= 64 {
			return 0, ErrIntOverflow
		}
		if m.index >= m.length {
			return 0, io.ErrUnexpectedEOF
		}
		b := m.data[m.index]
		m.index++
		val |= uint64(b&0x7F) << shift
		if b < 0x80 {
			break
		}
	}

	return val, nil
}

// Double values are encoded as a fixed length of 8 bytes in their IEEE-754 format.
func (m *Message) Double() (float64, error) {
	v, err := m.Fixed64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// Float values are encoded as a fixed length of 4 bytes in their IEEE-754 format.
func (m *Message) Float() (float32, error) {
	v, err := m.Fixed32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

// Int32 reads a variable-length encoding of up to 4 bytes. This field type is
// best used if the field only has positive numbers, otherwise use sint32.
// Note, this field can also by read as an Int64. TODO: test
func (m *Message) Int32() (int32, error) {
	v, err := m.Varint64()
	if err != nil {
		return 0, err
	}

	return int32(v), nil
}

// Int64 reads a variable-length encoding of up to 8 bytes. This field type is
// best used if the field only has positive numbers, otherwise use sint64.
func (m *Message) Int64() (int64, error) {
	v, err := m.Varint64()
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

// Uint32 reads a variable-length encoding of up to 4 bytes.
func (m *Message) Uint32() (uint32, error) {
	// Uses variable-length encoding
	v, err := m.Varint32()
	if err != nil {
		return 0, err
	}

	return uint32(v), nil
}

// Uint64 reads a variable-length encoding of up to 8 bytes.
func (m *Message) Uint64() (uint64, error) {
	// Uses variable-length encoding
	v, err := m.Varint64()
	if err != nil {
		return 0, err
	}

	return uint64(v), nil
}

// Sint32 uses variable-length encoding with zig-zag encoding for signed values.
// This field type more efficiently encodes negative numbers than regular int32s.
func (m *Message) Sint32() (int32, error) {
	v, err := m.Varint32()
	if err != nil {
		return 0, err
	}

	return unZig32(v), nil
}

// Sint64 uses variable-length encoding with zig-zag encoding for signed values.
// This field type more efficiently encodes negative numbers than regular int64s.
func (m *Message) Sint64() (int64, error) {
	v, err := m.Varint64()
	if err != nil {
		return 0, err
	}

	return unZig64(v), nil
}

// Bool is encoded as 0x01 or 0x00 plus the field+type prefix byte. 2 bytes total.
func (m *Message) Bool() (bool, error) {
	v, err := m.Varint64()
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

// String reads a string type. This data will always contain UTF-8 encoded or
// 7-bit ASCII text.
func (m *Message) String() (string, error) {
	b, err := m.Bytes()
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// Bytes returns the encode sequence of bytes.
// NOTE: this value is NOT copied.
func (m *Message) Bytes() ([]byte, error) {
	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	b := m.data[m.index : m.index+l]
	m.index += l
	return b, nil
}

func unZig32(v uint32) int32 {
	return int32((v >> 1) ^ uint32((int32(v&1)<<31)>>31))
}

func unZig64(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

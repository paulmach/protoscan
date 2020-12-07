package protoscan

// An Iterator allows for moving across a packed repeated field
// in a 'controlled' fashion.
type Iterator struct {
	base

	fieldNumber int
}

// Iterator will use the current field. The field must be a packed
// repeated field.
func (m *Message) Iterator(iter *Iterator) (*Iterator, error) {
	// TODO: validate wiretype makes sense

	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	if iter == nil {
		iter = &Iterator{}
	}
	iter.base = base{
		data:  m.data[m.index : m.index+l],
		index: 0,
	}
	iter.fieldNumber = m.fieldNumber
	m.index += l

	return iter, nil
}

// HasNext is used in a 'for' loop to read through all the elements.
// Returns false when all the items have been read.
// This method does NOT need to be called, reading a value automatically
// moves in the index forward. This behavior is different than Message.Next().
func (i *Iterator) HasNext() bool {
	return i.base.index < len(i.base.data)
}

// Count returns the total number of values in this repeated field.
// The answer depends on the type/encoding or the field:
// double, float, fixed, sfixed are WireType32bit or WireType64bit,
// all others int, uint, sint types are WireTypeVarint.
// The function will panic for any other value.
func (i *Iterator) Count(wireType int) int {
	if wireType == WireType32bit {
		return len(i.base.data) / 4
	}
	if wireType == WireType64bit {
		return len(i.base.data) / 8
	}
	if wireType == WireTypeVarint {
		var count int
		for _, b := range i.data {
			if b < 128 {
				count++
			}
		}

		return count
	}

	panic("invalid wire type for a packed repeated field")
}

// FieldNumber returns the number for the current repeated field.
// These numbers are defined in the protobuf definition file used to encode the message.
func (i *Iterator) FieldNumber() int {
	return i.fieldNumber
}

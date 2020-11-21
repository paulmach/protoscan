package protoscan

// RepeatedInt64 will append to the buffer the repeated values.
// This method supports packed or unpacked encoding.
func (m *Message) RepeatedInt64(buf []int64) ([]int64, error) {
	if m.wireType == WireTypeVarint {
		v, err := m.Int64()
		if err != nil {
			return nil, err
		}

		return append(buf, v), nil
	}

	l, err := m.packedLength()
	if err != nil {
		return nil, err
	}

	// if provided we append.
	if buf == nil {
		buf = make([]int64, 0, m.count(l))
	}

	postIndex := m.index + l
	for m.index < postIndex {
		v, err := m.Int64()
		if err != nil {
			return nil, err
		}

		buf = append(buf, v)
	}

	return buf, nil
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

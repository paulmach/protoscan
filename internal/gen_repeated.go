package main

import (
	"fmt"
	"os"
)

const tmpl = `
// Repeated%[1]s will append the repeated value(s) to the buffer.
// This method supports packed or unpacked encoding.
func (m *Message) Repeated%[1]s(buf []%[2]s) ([]%[2]s, error) {
	if m.wireType == %[3]s {
		v, err := m.%[1]s()
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
		buf = make([]%[2]s, 0, m.count(l))
	}

	postIndex := m.index + l
	for m.index < postIndex {
		v, err := m.%[1]s()
		if err != nil {
			return nil, err
		}

		buf = append(buf, v)
	}

	return buf, nil
}
`

var types = [][]string{
	{"Double", "float64", "WireType64bit"},
	{"Float", "float32", "WireType32bit"},
	{"Int32", "int32", "WireTypeVarint"},
	{"Int64", "int64", "WireTypeVarint"},
	{"Uint32", "uint32", "WireTypeVarint"},
	{"Uint64", "uint64", "WireTypeVarint"},
	{"Sint32", "int32", "WireTypeVarint"},
	{"Sint64", "int64", "WireTypeVarint"},
	{"Fixed32", "uint32", "WireType32bit"},
	{"Fixed64", "uint64", "WireType64bit"},
	{"Sfixed32", "int32", "WireType32bit"},
	{"Sfixed64", "int64", "WireType64bit"},
	{"Bool", "bool", "WireTypeVarint"},
}

func main() {
	f, err := os.OpenFile("repeated.go", os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(f, "package protoscan\n")

	for _, t := range types {
		fmt.Fprintf(f, tmpl, t[0], t[1], t[2])
	}
}

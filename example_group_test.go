package protoscan

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

func Example_groups() {

	data := []byte{}

	data = protowire.AppendTag(data, 200, WireTypeStartGroup)
	data = protowire.AppendTag(data, 300, WireType64bit)
	data = protowire.AppendFixed64(data, 100_100_100)
	data = protowire.AppendTag(data, 400, WireTypeVarint)
	data = protowire.AppendVarint(data, 100_100)
	data = protowire.AppendTag(data, 200, WireTypeEndGroup)

	var (
		groupFieldNum = 200
		groupData     []byte
	)

	msg := New(data)
	for msg.Next() {
		if msg.FieldNumber() == groupFieldNum && msg.WireType() == WireTypeStartGroup {
			start := msg.Index
			end := msg.Index
			for msg.Next() {
				msg.Skip()
				if msg.FieldNumber() == groupFieldNum && msg.WireType() == WireTypeEndGroup {
					break
				}
				end = msg.Index
			}
			// groupData would be the raw protobuf encoded bytes of the fields in the group.
			groupData = msg.Data[start:end]
		}
	}

	fmt.Printf("data length: %d\n", len(data))
	fmt.Printf("group data length: %v\n", len(groupData))

	// Output:
	// data length: 19
	// group data length: 15
}

func Example_emptyGroup() {
	data := []byte{}

	data = protowire.AppendTag(data, 100, WireType64bit)
	data = protowire.AppendFixed64(data, 100_100_100)
	data = protowire.AppendTag(data, 200, WireTypeStartGroup)
	data = protowire.AppendTag(data, 200, WireTypeEndGroup)
	data = protowire.AppendTag(data, 400, WireTypeVarint)
	data = protowire.AppendVarint(data, 100_100)

	var (
		groupFieldNum = 200
		groupData     []byte
	)

	msg := New(data)
	for msg.Next() {
		if msg.FieldNumber() == groupFieldNum && msg.WireType() == WireTypeStartGroup {
			start := msg.Index
			end := msg.Index
			for msg.Next() {
				msg.Skip()
				if msg.FieldNumber() == groupFieldNum && msg.WireType() == WireTypeEndGroup {
					break
				}
				end = msg.Index
			}
			// groupData would be the raw protobuf encoded bytes of the fields in the group.
			groupData = msg.Data[start:end]
		}
	}

	fmt.Printf("data length: %d\n", len(data))
	fmt.Printf("group data length: %v\n", len(groupData))

	// Output:
	// data length: 19
	// group data length: 0
}

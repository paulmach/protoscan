package protoscan_test

import (
	"fmt"

	"github.com/paulmach/protoscan"
	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

type Field struct {
	Number   int
	WireType int
	Data     []byte
}

// ReadIntoArray demonstrates how to get the raw data for each field in the message.
func Example_readIntoArray() {
	child := &testmsg.Child{
		Number:  proto.Int64(123),
		Numbers: []int64{1, 2, 3, -4, -5, -6, 7, 8},
		Grandchild: []*testmsg.Grandchild{
			{
				Number:  proto.Int64(111),
				Numbers: []int64{-1, 2, -3, 4, -5, 6, -7, 8},
			},
			{
				Number:  proto.Int64(-222),
				Numbers: []int64{1, -2, 3, -4, 5, -6, 7, -8},
			},
		},
		After: proto.Bool(true),
	}

	data, err := proto.Marshal(child)
	if err != nil {
		panic(err)
	}

	fields := []Field{}

	msg := protoscan.New(data)
	for msg.Next() {
		f := Field{
			Number:   msg.FieldNumber(),
			WireType: msg.WireType(),
		}
		start := msg.Index
		msg.Skip()
		end := msg.Index

		f.Data = msg.Data[start:end]
		fields = append(fields, f)
	}

	for _, f := range fields {
		fmt.Printf("%+v\n", f)
	}

	// Output:
	// {Number:100 WireType:0 Data:[123]}
	// {Number:200 WireType:2 Data:[50 192 62 111 130 125 44 255 255 255 255 255 255 255 255 255 1 2 253 255 255 255 255 255 255 255 255 1 4 251 255 255 255 255 255 255 255 255 1 6 249 255 255 255 255 255 255 255 255 1 8]}
	// {Number:200 WireType:2 Data:[59 192 62 162 254 255 255 255 255 255 255 255 1 130 125 44 1 254 255 255 255 255 255 255 255 255 1 3 252 255 255 255 255 255 255 255 255 1 5 250 255 255 255 255 255 255 255 255 1 7 248 255 255 255 255 255 255 255 255 1]}
	// {Number:300 WireType:0 Data:[1]}
	// {Number:300 WireType:0 Data:[2]}
	// {Number:300 WireType:0 Data:[3]}
	// {Number:300 WireType:0 Data:[252 255 255 255 255 255 255 255 255 1]}
	// {Number:300 WireType:0 Data:[251 255 255 255 255 255 255 255 255 1]}
	// {Number:300 WireType:0 Data:[250 255 255 255 255 255 255 255 255 1]}
	// {Number:300 WireType:0 Data:[7]}
	// {Number:300 WireType:0 Data:[8]}
	// {Number:3200 WireType:0 Data:[1]}
}

package protoscan

import (
	"log"
	"testing"

	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

func TestDecodeRepeated_numbers(t *testing.T) {
	tescases := []struct {
		name    string
		skip    int
		message *testmsg.Repeated
	}{
		{
			name: "int64",
			skip: 4,
			message: &testmsg.Repeated{
				I64:   []int64{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
	}

	for _, tc := range tescases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeRepeated(t, data, 0)
			compare(t, v, tc.message)
		})

		t.Run("skip "+tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeRepeated(t, data, tc.skip)
			compare(t, v, &testmsg.Scalar{After: proto.Bool(true)})
		})

		t.Run("packed "+tc.name, func(t *testing.T) {
			data, err := proto.Marshal(repeatedToPacked(tc.message))
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeRepeated(t, data, 0)
			compare(t, v, tc.message)
		})

		t.Run("skip packed "+tc.name, func(t *testing.T) {
			data, err := proto.Marshal(repeatedToPacked(tc.message))
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeRepeated(t, data, tc.skip)
			compare(t, v, &testmsg.Scalar{After: proto.Bool(true)})
		})
	}
}

func repeatedToPacked(r *testmsg.Repeated) *testmsg.Packed {
	return &testmsg.Packed{
		Dbl: r.Dbl, Flt: r.Flt,
		I32: r.I32, I64: r.I64,
		U32: r.U32, U64: r.U64,
		S32: r.S32, S64: r.S64,
		F32: r.F32, F64: r.F64,
		Sf32: r.Sf32, Sf64: r.Sf64,
		Bool:  r.Bool,
		Str:   r.Str,
		Byte:  r.Byte,
		After: r.After,
	}
}

func decodeRepeated(t *testing.T, data []byte, skip int) *testmsg.Repeated {
	msg := New(data)

	r := &testmsg.Repeated{}
	for msg.Scan() {
		if msg.FieldNumber() == skip {
			msg.Skip()
			continue
		}

		switch msg.FieldNumber() {
		case 4:
			v, err := msg.RepeatedInt64(r.I64)
			if err != nil {
				t.Fatalf("unable to read int64: %v", err)
			}
			r.I64 = v
		case 32:
			v, err := msg.Bool()
			if err != nil {
				t.Fatalf("unable to read after bool: %v", err)
			}
			r.After = &v
		default:
			log.Printf("skip")
			msg.Skip()
		}
	}

	return r
}

package protoscan

import (
	"io"
	"testing"

	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

func TestInterator(t *testing.T) {
	cases := []struct {
		name    string
		skip    int
		message *testmsg.Packed
	}{
		{
			name: "float",
			skip: 1,
			message: &testmsg.Packed{
				Flt:   []float32{1, 1.5, 2, 2.5, -3, -3.5},
				After: proto.Bool(true),
			},
		},
		{
			name: "int32",
			skip: 3,
			message: &testmsg.Packed{
				I32:   []int32{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "int64",
			skip: 4,
			message: &testmsg.Packed{
				I64:   []int64{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "uint32",
			skip: 5,
			message: &testmsg.Packed{
				U32:   []uint32{1, 2, 3, 4, 5, 6, 2000, 3000, 4000, 5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "uint64",
			skip: 6,
			message: &testmsg.Packed{
				U64:   []uint64{1, 2, 3, 4, 5, 6, 2000, 3000, 4000, 5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "sint32",
			skip: 7,
			message: &testmsg.Packed{
				S32:   []int32{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "sint64",
			skip: 8,
			message: &testmsg.Packed{
				S64:   []int64{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "fixed32",
			skip: 9,
			message: &testmsg.Packed{
				F32:   []uint32{1, 2, 3, 4, 5, 6, 2000, 3000, 4000, 5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "fixed64",
			skip: 10,
			message: &testmsg.Packed{
				F64:   []uint64{1, 2, 3, 4, 5, 6, 2000, 3000, 4000, 5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "sfixed32",
			skip: 11,
			message: &testmsg.Packed{
				Sf32:  []int32{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "sfixed64",
			skip: 12,
			message: &testmsg.Packed{
				Sf64:  []int64{1, -2, 3, -4, 5, 6, 2000, -3000, 4000, -5000},
				After: proto.Bool(true),
			},
		},
		{
			name: "bools",
			skip: 13,
			message: &testmsg.Packed{
				Bool:  []bool{true, true, false, false, true, false},
				After: proto.Bool(true),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeIterator(t, data, 0)
			compare(t, v, tc.message)
		})

		t.Run("skip "+tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeIterator(t, data, tc.skip)
			compare(t, v, &testmsg.Scalar{After: proto.Bool(true)})
		})

		t.Run("counts "+tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeIterator(t, data, 0)
			if len(v.Flt) != cap(v.Flt) {
				t.Errorf("incorrect counts: %v != %v", len(v.Flt), cap(v.Flt))
			}
			if len(v.Dbl) != cap(v.Dbl) {
				t.Errorf("incorrect counts: %v != %v", len(v.Dbl), cap(v.Dbl))
			}
			if len(v.I32) != cap(v.I32) {
				t.Errorf("incorrect counts: %v != %v", len(v.I32), cap(v.I32))
			}
			if len(v.I64) != cap(v.I64) {
				t.Errorf("incorrect counts: %v != %v", len(v.I64), cap(v.I64))
			}
			if len(v.U32) != cap(v.U32) {
				t.Errorf("incorrect counts: %v != %v", len(v.U32), cap(v.U32))
			}
			if len(v.U64) != cap(v.U64) {
				t.Errorf("incorrect counts: %v != %v", len(v.U64), cap(v.U64))
			}
			if len(v.S32) != cap(v.S32) {
				t.Errorf("incorrect counts: %v != %v", len(v.S32), cap(v.S32))
			}
			if len(v.F32) != cap(v.F32) {
				t.Errorf("incorrect counts: %v != %v", len(v.F32), cap(v.F32))
			}
			if len(v.F64) != cap(v.F64) {
				t.Errorf("incorrect counts: %v != %v", len(v.F64), cap(v.F64))
			}
			if len(v.Sf32) != cap(v.Sf32) {
				t.Errorf("incorrect counts: %v != %v", len(v.Sf32), cap(v.Sf32))
			}
			if len(v.Sf64) != cap(v.Sf64) {
				t.Errorf("incorrect counts: %v != %v", len(v.Sf64), cap(v.Sf64))
			}
			if len(v.Bool) != cap(v.Bool) {
				t.Errorf("incorrect counts: %v != %v", len(v.Bool), cap(v.Bool))
			}
			if len(v.Str) != cap(v.Str) {
				t.Errorf("incorrect counts: %v != %v", len(v.Str), cap(v.Str))
			}
			if len(v.Byte) != cap(v.Byte) {
				t.Errorf("incorrect counts: %v != %v", len(v.Byte), cap(v.Byte))
			}
		})
	}
}

func TestIterator_errors(t *testing.T) {
	message := &testmsg.Packed{
		I64: make([]int64, 4000),
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data[:2])
	if !msg.Next() {
		t.Fatalf("next is false?")
	}

	_, err = msg.Iterator(nil)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("incorrect error: %v", err)
	}
}

func TestIterator_Skip(t *testing.T) {
	message := &testmsg.Packed{
		Flt: make([]float32, 10),
		Dbl: make([]float64, 10),
		I32: make([]int32, 10),
		I64: make([]int64, 10),
	}

	for i := 0; i < 10; i++ {
		message.Flt[i] = float32(10 * i)
		message.Dbl[i] = float64(15 * i)
		message.I32[i] = int32(10 + 100*i)
	}

	message.I64[0] = int64(1 << 7)
	message.I64[2] = int64(1 << 15)
	message.I64[4] = int64(1 << 23)
	message.I64[6] = int64(1 << 23)

	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1: // Float
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to make iterator: %v", err)
			}

			iter.Skip(WireType32bit, 0)
			if v, _ := iter.Float(); v != 0 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireType32bit, 1)
			if v, _ := iter.Float(); v != 20 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireType32bit, 2)
			if v, _ := iter.Float(); v != 50 {
				t.Errorf("incorrect value: %v", v)
			}
		case 2:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to make iterator: %v", err)
			}

			iter.Skip(WireType64bit, 1)
			if v, _ := iter.Double(); v != 15 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireType64bit, 1)
			if v, _ := iter.Double(); v != 45 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireType64bit, 2)
			if v, _ := iter.Double(); v != 90 {
				t.Errorf("incorrect value: %v", v)
			}
		case 3:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to make iterator: %v", err)
			}

			iter.Skip(WireTypeVarint, 1)
			if v, _ := iter.Int32(); v != 110 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireTypeVarint, 1)
			if v, _ := iter.Int32(); v != 310 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireTypeVarint, 2)
			if v, _ := iter.Int32(); v != 610 {
				t.Errorf("incorrect value: %v", v)
			}
		case 4:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to make iterator: %v", err)
			}

			iter.Skip(WireTypeVarint, 0)
			if v, _ := iter.Int64(); v != 128 {
				t.Errorf("incorrect value: %v", v)
			}

			iter.Skip(WireTypeVarint, 2)
			if v, _ := iter.Int64(); v != 0 {
				t.Errorf("incorrect value: %v", v)
			}

			if v, _ := iter.Int64(); v != 0x0800000 {
				t.Errorf("incorrect value: %x", v)
			}
		default:
			msg.Skip()
		}
	}

	if err := msg.Err(); err != nil {
		t.Fatalf("read error: %v", err)
	}
}

func TestIterator_FieldNumber(t *testing.T) {
	message := &testmsg.Packed{
		I64: make([]int64, 4000),
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)
	if !msg.Next() {
		t.Fatalf("next is false?")
	}

	iter, err := msg.Iterator(nil)
	if err != nil {
		t.Fatalf("error getting iterator: %v", err)
	}

	if v := iter.FieldNumber(); v != 4 {
		t.Errorf("incorrect field number: %v", v)
	}
}

func decodeIterator(t *testing.T, data []byte, skip int) *testmsg.Packed {
	msg := New(data)

	p := &testmsg.Packed{}
	for msg.Next() {
		if msg.FieldNumber() == skip {
			msg.Skip()
			continue
		}

		switch msg.FieldNumber() {
		case 1:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.Flt = make([]float32, 0, iter.Count(WireType32bit))
			for iter.HasNext() {
				v, err := iter.Float()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.Flt = append(p.Flt, v)
			}
		case 2:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.Dbl = make([]float64, 0, iter.Count(WireType64bit))
			for iter.HasNext() {
				v, err := iter.Double()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.Dbl = append(p.Dbl, v)
			}
		case 3:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.I32 = make([]int32, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Int32()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.I32 = append(p.I32, v)
			}
		case 4:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.I64 = make([]int64, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Int64()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.I64 = append(p.I64, v)
			}
		case 5:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.U32 = make([]uint32, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Uint32()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.U32 = append(p.U32, v)
			}
		case 6:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.U64 = make([]uint64, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Uint64()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.U64 = append(p.U64, v)
			}
		case 7:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.S32 = make([]int32, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Sint32()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.S32 = append(p.S32, v)
			}
		case 8:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.S64 = make([]int64, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Sint64()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.S64 = append(p.S64, v)
			}
		case 9:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.F32 = make([]uint32, 0, iter.Count(WireType32bit))
			for iter.HasNext() {
				v, err := iter.Fixed32()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.F32 = append(p.F32, v)
			}
		case 10:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.F64 = make([]uint64, 0, iter.Count(WireType64bit))
			for iter.HasNext() {
				v, err := iter.Fixed64()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.F64 = append(p.F64, v)
			}
		case 11:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.Sf32 = make([]int32, 0, iter.Count(WireType32bit))
			for iter.HasNext() {
				v, err := iter.Sfixed32()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.Sf32 = append(p.Sf32, v)
			}
		case 12:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.Sf64 = make([]int64, 0, iter.Count(WireType64bit))
			for iter.HasNext() {
				v, err := iter.Sfixed64()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.Sf64 = append(p.Sf64, v)
			}
		case 13:
			iter, err := msg.Iterator(nil)
			if err != nil {
				t.Fatalf("unable to create iterator: %v", err)
			}

			p.Bool = make([]bool, 0, iter.Count(WireTypeVarint))
			for iter.HasNext() {
				v, err := iter.Bool()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.Bool = append(p.Bool, v)
			}
		case 32:
			v, err := msg.Bool()
			if err != nil {
				t.Fatalf("unable to read after bool: %v", err)
			}
			p.After = &v
		default:
			msg.Skip()
		}
	}

	if err := msg.Err(); err != nil {
		t.Fatalf("scanning error: %v", err)
	}

	return p
}

func BenchmarkRepeatedInt64(b *testing.B) {
	items := []int64{}
	for i := 0; i < 100; i++ {
		items = append(items, 50*int64(i))
	}

	data, err := proto.Marshal(&testmsg.Packed{I64: items})
	if err != nil {
		b.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.Index = 0
		for msg.Next() {
			switch msg.FieldNumber() {
			case 4:
				_, err := msg.RepeatedInt64(nil)
				if err != nil {
					b.Fatalf("unable to read: %v", err)
				}
			default:
				msg.Skip()
			}
		}
	}
}

func BenchmarkIterateInt64(b *testing.B) {
	items := []int64{}
	for i := 0; i < 100; i++ {
		items = append(items, int64(50*i))
	}

	data, err := proto.Marshal(&testmsg.Packed{I64: items})
	if err != nil {
		b.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)
	iter := &Iterator{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.Index = 0
		for msg.Next() {
			switch msg.FieldNumber() {
			case 4:
				var err error
				iter, err := msg.Iterator(iter)
				if err != nil {
					b.Fatalf("unable to create iterator: %v", err)
				}

				c := iter.Count(WireTypeVarint)
				if c != 100 {
					b.Errorf("incorrect count, got %v", c)
				}

				data := make([]int64, 0, iter.Count(WireTypeVarint))
				for iter.HasNext() {
					v, err := iter.Int64()
					if err != nil {
						b.Fatalf("unable to read: %v", err)
					}
					data = append(data, v)
				}

				if len(data) != 100 {
					b.Error("incorrect data")
				}
			default:
				msg.Skip()
			}
		}
	}
}

func BenchmarkIterateSkip_single(b *testing.B) {
	items := []int64{}
	for i := 0; i < 100; i++ {
		items = append(items, int64(50*i))
	}

	data, err := proto.Marshal(&testmsg.Packed{I64: items})
	if err != nil {
		b.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)
	iter := &Iterator{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.Index = 0
		for msg.Next() {
			switch msg.FieldNumber() {
			case 4:
				var err error
				iter, err := msg.Iterator(iter)
				if err != nil {
					b.Fatalf("unable to create iterator: %v", err)
				}

				// to match InterateInt64 benchmark
				_ = make([]int64, 0, iter.Count(WireTypeVarint))
				for iter.HasNext() {
					iter.Skip(WireTypeVarint, 1)
				}
			default:
				msg.Skip()
			}
		}
	}
}

func BenchmarkIterateSkip_all(b *testing.B) {
	items := []int64{}
	for i := 0; i < 100; i++ {
		items = append(items, int64(50*i))
	}

	data, err := proto.Marshal(&testmsg.Packed{I64: items})
	if err != nil {
		b.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)
	iter := &Iterator{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.Index = 0
		for msg.Next() {
			switch msg.FieldNumber() {
			case 4:
				var err error
				iter, err := msg.Iterator(iter)
				if err != nil {
					b.Fatalf("unable to create iterator: %v", err)
				}

				// to match IterateInt64 benchmark
				c := iter.Count(WireTypeVarint)
				_ = make([]int64, 0, c)
				iter.Skip(WireTypeVarint, c)
			default:
				msg.Skip()
			}
		}
	}
}

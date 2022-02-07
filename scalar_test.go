package protoscan

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

func TestDecodeScalar_numbers(t *testing.T) {
	cases := []struct {
		name    string
		message *testmsg.Scalar
	}{
		{
			name:    "positive float",
			message: &testmsg.Scalar{Flt: proto.Float32(123.4567)},
		},
		{
			name:    "negative float",
			message: &testmsg.Scalar{Flt: proto.Float32(-23.4567)},
		},
		{
			name:    "zero float",
			message: &testmsg.Scalar{Flt: proto.Float32(0)},
		},
		{
			name:    "positive double",
			message: &testmsg.Scalar{Dbl: proto.Float64(123.4567)},
		},
		{
			name:    "negative double",
			message: &testmsg.Scalar{Dbl: proto.Float64(-23.4567)},
		},
		{
			name:    "zero double",
			message: &testmsg.Scalar{Dbl: proto.Float64(0)},
		},
		{
			name:    "float and double",
			message: &testmsg.Scalar{Dbl: proto.Float64(123.4567), Flt: proto.Float32(34)},
		},

		{
			name:    "positive int32",
			message: &testmsg.Scalar{I32: proto.Int32(5280)},
		},
		{
			name:    "negative int32",
			message: &testmsg.Scalar{I32: proto.Int32(-123_567_890)},
		},
		{
			name:    "zero int32",
			message: &testmsg.Scalar{I32: proto.Int32(0)},
		},
		{
			name:    "positive int64",
			message: &testmsg.Scalar{I64: proto.Int64(5280)},
		},
		{
			name:    "big int64",
			message: &testmsg.Scalar{I64: proto.Int64(9_828_385_280)},
		},
		{
			name:    "negative int64",
			message: &testmsg.Scalar{I64: proto.Int64(-111_123_567_890)},
		},
		{
			name:    "zero int64",
			message: &testmsg.Scalar{I64: proto.Int64(0)},
		},

		{
			name:    "positive uint32",
			message: &testmsg.Scalar{U32: proto.Uint32(5280)},
		},
		{
			name:    "zero uint32",
			message: &testmsg.Scalar{U32: proto.Uint32(0)},
		},
		{
			name:    "positive uint64",
			message: &testmsg.Scalar{U64: proto.Uint64(5280)},
		},
		{
			name:    "big uint64",
			message: &testmsg.Scalar{U64: proto.Uint64(9_828_385_280)},
		},
		{
			name:    "zero uint64",
			message: &testmsg.Scalar{U64: proto.Uint64(0)},
		},

		{
			name:    "positive sint32",
			message: &testmsg.Scalar{S32: proto.Int32(5280)},
		},
		{
			name:    "negative sint32",
			message: &testmsg.Scalar{S32: proto.Int32(-123_567_890)},
		},
		{
			name:    "zero sint32",
			message: &testmsg.Scalar{S32: proto.Int32(0)},
		},
		{
			name:    "positive sint64",
			message: &testmsg.Scalar{S64: proto.Int64(5280)},
		},
		{
			name:    "big sint64",
			message: &testmsg.Scalar{S64: proto.Int64(9_828_385_280)},
		},
		{
			name:    "negative sint64",
			message: &testmsg.Scalar{S64: proto.Int64(-111_123_567_890)},
		},
		{
			name:    "zero sint64",
			message: &testmsg.Scalar{S64: proto.Int64(0)},
		},

		{
			name:    "positive fixed32",
			message: &testmsg.Scalar{F32: proto.Uint32(5280)},
		},
		{
			name:    "zero fixed32",
			message: &testmsg.Scalar{F32: proto.Uint32(0)},
		},
		{
			name:    "positive fixed64",
			message: &testmsg.Scalar{F64: proto.Uint64(5280)},
		},
		{
			name:    "big fixed64",
			message: &testmsg.Scalar{F64: proto.Uint64(9_828_385_280)},
		},
		{
			name:    "zero fixed64",
			message: &testmsg.Scalar{F64: proto.Uint64(0)},
		},

		{
			name:    "positive sfixed32",
			message: &testmsg.Scalar{Sf32: proto.Int32(5280)},
		},
		{
			name:    "negative sfixed32",
			message: &testmsg.Scalar{Sf32: proto.Int32(-5280)},
		},
		{
			name:    "zero sfixed32",
			message: &testmsg.Scalar{Sf32: proto.Int32(0)},
		},
		{
			name:    "positive sfixed64",
			message: &testmsg.Scalar{Sf64: proto.Int64(5280)},
		},
		{
			name:    "negative sfixed64",
			message: &testmsg.Scalar{Sf64: proto.Int64(-1_234_567)},
		},
		{
			name:    "big sfixed64",
			message: &testmsg.Scalar{Sf64: proto.Int64(9_828_385_280)},
		},
		{
			name:    "zero sfixed64",
			message: &testmsg.Scalar{Sf64: proto.Int64(0)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			v := decodeScalar(t, data, 0)
			compare(t, v, tc.message)
		})
	}
}

func TestDecodeScalar_bool(t *testing.T) {
	cases := []struct {
		name    string
		message *testmsg.Scalar
	}{
		{
			name:    "true",
			message: &testmsg.Scalar{Bool: proto.Bool(true)},
		},
		{
			name:    "false",
			message: &testmsg.Scalar{Bool: proto.Bool(false)},
		},
		{
			name:    "none",
			message: &testmsg.Scalar{Bool: nil},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			r := &testmsg.Scalar{}
			proto.Unmarshal(data, r)

			v := decodeScalar(t, data, 0)
			compare(t, v, tc.message)
		})
	}
}

func TestDecodeScalar_string(t *testing.T) {
	cases := []struct {
		name    string
		message *testmsg.Scalar
	}{
		{
			name:    "empty",
			message: &testmsg.Scalar{Str: proto.String("")},
		},
		{
			name:    "present",
			message: &testmsg.Scalar{Str: proto.String("message")},
		},
		{
			name:    "emoji",
			message: &testmsg.Scalar{Str: proto.String("123 😃 456")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			r := &testmsg.Scalar{}
			proto.Unmarshal(data, r)

			v := decodeScalar(t, data, 0)
			compare(t, v, tc.message)
		})
	}
}

func TestDecodeScalar_bytes(t *testing.T) {
	cases := []struct {
		name    string
		message *testmsg.Scalar
	}{
		{
			name:    "empty",
			message: &testmsg.Scalar{Byte: []byte{}},
		},
		{
			name:    "empty",
			message: &testmsg.Scalar{Byte: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		},
		{
			name:    "long",
			message: &testmsg.Scalar{Byte: make([]byte, 10000)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.message)
			if err != nil {
				t.Fatalf("unable to marshal: %v", err)
			}

			r := &testmsg.Scalar{}
			proto.Unmarshal(data, r)

			v := decodeScalar(t, data, 0)
			compare(t, v, tc.message)
		})
	}
}

func TestDecodeScalar_skip(t *testing.T) {
	cases := []struct {
		name    string
		skip    int
		message *testmsg.Scalar
	}{
		{
			name:    "skip float",
			skip:    1,
			message: &testmsg.Scalar{Flt: proto.Float32(1.5), After: proto.Bool(true)},
		},
		{
			name:    "skip double",
			skip:    2,
			message: &testmsg.Scalar{Dbl: proto.Float64(1.5), After: proto.Bool(true)},
		},
		{
			name:    "skip int32",
			skip:    3,
			message: &testmsg.Scalar{I32: proto.Int32(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip int64",
			skip:    4,
			message: &testmsg.Scalar{I64: proto.Int64(-1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip uint32",
			skip:    5,
			message: &testmsg.Scalar{U32: proto.Uint32(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip uint64",
			skip:    6,
			message: &testmsg.Scalar{U64: proto.Uint64(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip sint32",
			skip:    7,
			message: &testmsg.Scalar{S32: proto.Int32(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip sint64",
			skip:    8,
			message: &testmsg.Scalar{S64: proto.Int64(-1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip fixed32",
			skip:    9,
			message: &testmsg.Scalar{F32: proto.Uint32(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip fixed64",
			skip:    10,
			message: &testmsg.Scalar{F64: proto.Uint64(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip sfixed32",
			skip:    11,
			message: &testmsg.Scalar{Sf32: proto.Int32(1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip sfixed64",
			skip:    12,
			message: &testmsg.Scalar{Sf64: proto.Int64(-1_234_567), After: proto.Bool(true)},
		},
		{
			name:    "skip bool",
			skip:    13,
			message: &testmsg.Scalar{Bool: proto.Bool(false), After: proto.Bool(true)},
		},
		{
			name: "skip string",
			skip: 14,
			message: &testmsg.Scalar{
				Str:   proto.String("abcdefghij qwerty"),
				After: proto.Bool(true),
			},
		},
		{
			name: "skip bytes",
			skip: 15,
			message: &testmsg.Scalar{
				Byte:  make([]byte, 10000),
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

			r := &testmsg.Scalar{}
			proto.Unmarshal(data, r)

			v := decodeScalar(t, data, tc.skip)
			compare(t, v, &testmsg.Scalar{After: proto.Bool(true)})
		})
	}
}

func TestMessage_Varint32(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		msg := New([]byte{230, 230, 230, 230, 230, 230})
		_, err := msg.Varint32()
		if err != ErrIntOverflow {
			t.Errorf("wrong error: %v", err)
		}
	})

	t.Run("end of input", func(t *testing.T) {
		msg := New([]byte{230, 230})
		_, err := msg.Varint32()
		if err != io.ErrUnexpectedEOF {
			t.Errorf("wrong error: %v", err)
		}
	})
}

func TestMessage_Varint64(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		msg := New([]byte{230, 230, 230, 230, 230, 230, 230, 230, 230, 230})
		_, err := msg.Varint64()
		if err != ErrIntOverflow {
			t.Errorf("wrong error: %v", err)
		}
	})

	t.Run("end of input", func(t *testing.T) {
		msg := New([]byte{230, 230})
		_, err := msg.Varint64()
		if err != io.ErrUnexpectedEOF {
			t.Errorf("wrong error: %v", err)
		}
	})
}

func TestMessage_Int32(t *testing.T) {
	// tests reading an int32 field as an int64
	message := &testmsg.Scalar{
		I32: proto.Int32(1_234_567),
	}

	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	r := &testmsg.Scalar{}
	proto.Unmarshal(data, r)

	msg := New(data)

	var val int64
	for msg.Next() {
		if msg.FieldNumber() == 3 {
			v, err := msg.Int64()
			if err != nil {
				t.Fatalf("unable to read: %v", err)
			}

			val = v
		} else {
			msg.Skip()
		}
	}

	if val != 1_234_567 {
		t.Errorf("incorrect value: %d != %d", val, 1_234_567)
	}
}

func decodeScalar(t testing.TB, data []byte, skip int) *testmsg.Scalar {
	msg := New(data)

	s := &testmsg.Scalar{}
	for msg.Next() {
		if msg.FieldNumber() == skip {
			msg.Skip()
			continue
		}

		switch msg.FieldNumber() {
		case 1:
			v, err := msg.Float()
			if err != nil {
				t.Fatalf("unable to read float: %v", err)
			}
			s.Flt = &v
		case 2:
			v, err := msg.Double()
			if err != nil {
				t.Fatalf("unable to read double: %v", err)
			}
			s.Dbl = &v
		case 3:
			v, err := msg.Int32()
			if err != nil {
				t.Fatalf("unable to read int32: %v", err)
			}
			s.I32 = &v
		case 4:
			v, err := msg.Int64()
			if err != nil {
				t.Fatalf("unable to read int64: %v", err)
			}
			s.I64 = &v
		case 5:
			v, err := msg.Uint32()
			if err != nil {
				t.Fatalf("unable to read uint32: %v", err)
			}
			s.U32 = &v
		case 6:
			v, err := msg.Uint64()
			if err != nil {
				t.Fatalf("unable to read uint64: %v", err)
			}
			s.U64 = &v
		case 7:
			v, err := msg.Sint32()
			if err != nil {
				t.Fatalf("unable to read sint32: %v", err)
			}
			s.S32 = &v
		case 8:
			v, err := msg.Sint64()
			if err != nil {
				t.Fatalf("unable to read sint64: %v", err)
			}
			s.S64 = &v
		case 9:
			v, err := msg.Fixed32()
			if err != nil {
				t.Fatalf("unable to read fixed32: %v", err)
			}
			s.F32 = &v
		case 10:
			v, err := msg.Fixed64()
			if err != nil {
				t.Fatalf("unable to read fixed64: %v", err)
			}
			s.F64 = &v
		case 11:
			v, err := msg.Sfixed32()
			if err != nil {
				t.Fatalf("unable to read sfixed32: %v", err)
			}
			s.Sf32 = &v
		case 12:
			v, err := msg.Sfixed64()
			if err != nil {
				t.Fatalf("unable to read sfixed64: %v", err)
			}
			s.Sf64 = &v
		case 13:
			v, err := msg.Bool()
			if err != nil {
				t.Fatalf("unable to read bool: %v", err)
			}
			s.Bool = &v
		case 14:
			v, err := msg.String()
			if err != nil {
				t.Fatalf("unable to read string: %v", err)
			}
			s.Str = &v
		case 15:
			v, err := msg.Bytes()
			if err != nil {
				t.Fatalf("unable to read bytes: %v", err)
			}
			s.Byte = v
		case 32:
			v, err := msg.Bool()
			if err != nil {
				t.Fatalf("unable to read after bool: %v", err)
			}
			s.After = &v
		default:
			msg.Skip()
		}
	}

	if err := msg.Err(); err != nil {
		t.Fatalf("scanning error: %v", err)
	}

	return s
}

func compare(t *testing.T, v, expected interface{}) {
	t.Helper()

	// the private fields mess with reflect.DeepEqual so json marshalling.

	vd, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	ed, err := json.Marshal(expected)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	if bytes.Compare(vd, ed) != 0 {
		t.Logf("%v", string(vd))
		t.Logf("%v", string(ed))
		t.Errorf("results not equal")
	}
}

var bscalar = &testmsg.Scalar{
	Flt:  proto.Float32(1_234_567),
	Dbl:  proto.Float64(1_234_567),
	I32:  proto.Int32(1_234_567),
	I64:  proto.Int64(1_234_567),
	U32:  proto.Uint32(1_234_567),
	U64:  proto.Uint64(1_234_567),
	S32:  proto.Int32(1_234_567),
	S64:  proto.Int64(1_234_567),
	F32:  proto.Uint32(1_234_567),
	F64:  proto.Uint64(1_234_567),
	Sf32: proto.Int32(1_234_567),
	Sf64: proto.Int64(1_234_567),
}

func BenchmarkScalar_standard(b *testing.B) {
	data, err := proto.Marshal(bscalar)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := &testmsg.Scalar{}
		proto.Unmarshal(data, r)
	}
}

func BenchmarkScalar_protoscan(b *testing.B) {
	data, err := proto.Marshal(bscalar)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decodeScalar(b, data, 0)
	}
}

func BenchmarkVarint32(b *testing.B) {
	m := New([]byte{200, 199, 198, 6, 0, 0, 0, 0})

	// test it out
	v, err := m.Varint32()
	if err != nil {
		b.Fatal(err)
	}
	if v != 13738952 {
		b.Fatalf("incorrect value %v != 13738952", v)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Index = 0
		_, err := m.Varint32()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInt64(b *testing.B) {
	m := New([]byte{200, 199, 198, 6, 0, 0, 0, 0})

	// test it out
	v, err := m.Int64()
	if err != nil {
		b.Fatal(err)
	}
	if v != 13738952 {
		b.Fatalf("incorrect value %v != 13738952", v)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Index = 0
		_, err := m.Int64()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBool(b *testing.B) {
	m := New([]byte{1, 0, 1, 0, 0, 0, 0, 0})

	// test it out
	v, err := m.Bool()
	if err != nil {
		b.Fatal(err)
	}
	if !v {
		b.Fatalf("incorrect bool")
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Index = 0
		_, err := m.Bool()
		if err != nil {
			b.Fatal(err)
		}
	}
}

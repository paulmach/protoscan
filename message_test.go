package protoscan

import (
	"io"
	"testing"

	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

func TestMessage_Message(t *testing.T) {
	parent := &testmsg.Parent{
		Child: &testmsg.Child{
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
		},
		After: proto.Bool(true),
	}

	data, err := proto.Marshal(parent)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	t.Run("decode everything", func(t *testing.T) {
		msg := New(data)
		p := &testmsg.Parent{}

		for msg.Next() {
			switch msg.FieldNumber() {
			case 1:
				cmsg, err := msg.Message()
				if err != nil {
					t.Fatalf("unable to read message: %v", err)
				}

				p.Child = &testmsg.Child{}
				for cmsg.Next() {
					switch cmsg.FieldNumber() {
					case 100:
						v, err := cmsg.Int64()
						if err != nil {
							t.Fatalf("unable to read: %v", err)
						}
						p.Child.Number = &v
					case 200:
						gcmsg, err := cmsg.Message()
						if err != nil {
							t.Fatalf("unable to read: %v", err)
						}

						gc := &testmsg.Grandchild{}
						for gcmsg.Next() {
							switch gcmsg.FieldNumber() {
							case 1000:
								v, err := gcmsg.Int64()
								if err != nil {
									t.Fatalf("unable to read: %v", err)
								}
								gc.Number = &v
							case 2000:
								v, err := gcmsg.RepeatedInt64(gc.Numbers)
								if err != nil {
									t.Fatalf("unable to read: %v", err)
								}
								gc.Numbers = v
							case 32000:
								v, err := gcmsg.Bool()
								if err != nil {
									t.Fatalf("unable to read: %v", err)
								}
								gc.After = &v
							default:
								gcmsg.Skip()
							}
						}

						p.Child.Grandchild = append(p.Child.Grandchild, gc)
					case 300:
						v, err := cmsg.RepeatedInt64(p.Child.Numbers)
						if err != nil {
							t.Fatalf("unable to read: %v", err)
						}
						p.Child.Numbers = v
					case 3200:
						v, err := cmsg.Bool()
						if err != nil {
							t.Fatalf("unable to read: %v", err)
						}
						p.Child.After = &v
					default:
						cmsg.Skip()
					}
				}
			case 32:
				v, err := msg.Bool()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.After = &v
			default:
				msg.Skip()
			}
		}

		if err := msg.Err(); err != nil {
			t.Fatalf("scanning error: %v", err)
		}

		compare(t, p, parent)
	})

	t.Run("decode only part of the message", func(t *testing.T) {
		msg := New(data)
		p := &testmsg.Parent{}

		for msg.Next() {
			switch msg.FieldNumber() {
			case 1:
				_, err := msg.Message()
				if err != nil {
					t.Fatalf("unable to read message: %v", err)
				}

				// don't do anything with the message
			case 32:
				v, err := msg.Bool()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.After = &v
			default:
				msg.Skip()
			}
		}

		if err := msg.Err(); err != nil {
			t.Fatalf("scanning error: %v", err)
		}

		if *p.After != true {
			t.Errorf("should not require complete read of message")
		}
	})

	t.Run("skip the message", func(t *testing.T) {
		msg := New(data)
		p := &testmsg.Parent{}

		for msg.Next() {
			switch msg.FieldNumber() {
			case 1:
				msg.Skip()
			case 32:
				v, err := msg.Bool()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.After = &v
			default:
				msg.Skip()
			}
		}

		if err := msg.Err(); err != nil {
			t.Fatalf("scanning error: %v", err)
		}

		if *p.After != true {
			t.Errorf("should skip embedded messages")
		}
	})
}

func TestMessage_Next(t *testing.T) {
	// read err should be false and set error
	msg := New([]byte{201, 200, 200, 200, 200, 200, 200, 200, 200, 200})

	if msg.Next() {
		t.Errorf("should be false on if error")
	}

	if err := msg.Err(); err != ErrIntOverflow {
		t.Errorf("incorrect error: %v", err)
	}
}

func TestMessage_Skip(t *testing.T) {
	// error with wire type 1, 64 bit
	msg := New([]byte{0x10 | WireType64bit, 0x05})

	msg.Next()
	msg.Skip()

	if msg.Next() {
		t.Errorf("should be false on if error")
	}

	if err := msg.Err(); err != io.ErrUnexpectedEOF {
		t.Errorf("incorrect error: %v", err)
	}

	// error with wire type 2, length delimited
	msg.Reset([]byte{0x10 | WireTypeLengthDelimited, 0x85, 0x04})

	msg.Next()
	msg.Skip()

	if msg.Next() {
		t.Errorf("should be false on if error")
	}

	if err := msg.Err(); err != io.ErrUnexpectedEOF {
		t.Errorf("incorrect error: %v", err)
	}

	// error with wire type 5, 32 bit
	msg.Reset([]byte{0x10 | WireType32bit, 0x85, 0x04})

	msg.Next()
	msg.Skip()

	if msg.Next() {
		t.Errorf("should be false on if error")
	}

	if err := msg.Err(); err != io.ErrUnexpectedEOF {
		t.Errorf("incorrect error: %v", err)
	}
}

func TestMessage_MessageData(t *testing.T) {
	parent := &testmsg.Parent{
		Child: &testmsg.Child{
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
			After: proto.Bool(false),
		},
		After: proto.Bool(false),
	}

	data, err := proto.Marshal(parent)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	t.Run("decode using golang/protobuf", func(t *testing.T) {
		msg := New(data)
		p := &testmsg.Parent{}

		for msg.Next() {
			switch msg.FieldNumber() {
			case 1:
				d, err := msg.MessageData()
				if err != nil {
					t.Fatalf("unable to read message: %v", err)
				}

				p.Child = &testmsg.Child{}
				err = proto.Unmarshal(d, p.Child)
				if err != nil {
					t.Fatalf("unable to unmarshal: %v", err)
				}
			case 32:
				v, err := msg.Bool()
				if err != nil {
					t.Fatalf("unable to read: %v", err)
				}
				p.After = &v
			default:
				t.Fatalf("no skips in this message: field number %d", msg.FieldNumber())
			}
		}

		if err := msg.Err(); err != nil {
			t.Fatalf("scanning error: %v", err)
		}

		compare(t, p, parent)
	})

	t.Run("invalid packed length", func(t *testing.T) {
		msg := New([]byte{200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200})
		_, err := msg.MessageData()

		if err != ErrIntOverflow {
			t.Errorf("incorrect error: %v", err)
		}
	})
}

func TestMessage_Reset(t *testing.T) {
	message1 := &testmsg.Scalar{Flt: proto.Float32(123.4567)}

	data, err := proto.Marshal(message1)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	msg := New(data)

	s := &testmsg.Scalar{}
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1:
			if v := msg.WireType(); v != WireType32bit {
				t.Errorf("incorrect wiretype: %v", v)
			}

			v, err := msg.Float()
			if err != nil {
				t.Fatalf("unable to read float: %v", err)
			}
			s.Flt = &v
		default:
			msg.Skip()
		}
	}
	compare(t, s, message1)

	// Reset
	msg.Reset(nil)

	s = &testmsg.Scalar{}
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1:
			v, err := msg.Float()
			if err != nil {
				t.Fatalf("unable to read float: %v", err)
			}
			s.Flt = &v
		default:
			msg.Skip()
		}
	}
	compare(t, s, message1)

	// Reset with new data
	message2 := &testmsg.Scalar{Flt: proto.Float32(55.555)}

	data2, err := proto.Marshal(message2)
	if err != nil {
		t.Fatalf("unable to marshal: %v", err)
	}

	msg.Reset(data2)

	s = &testmsg.Scalar{}
	for msg.Next() {
		switch msg.FieldNumber() {
		case 1:
			v, err := msg.Float()
			if err != nil {
				t.Fatalf("unable to read float: %v", err)
			}
			s.Flt = &v
		default:
			msg.Skip()
		}
	}
	compare(t, s, message2)
}

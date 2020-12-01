package protoscan

import (
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

		for msg.Scan() {
			switch msg.FieldNumber() {
			case 1:
				cmsg, err := msg.Message()
				if err != nil {
					t.Fatalf("unable to read message: %v", err)
				}

				p.Child = &testmsg.Child{}
				for cmsg.Scan() {
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
						for gcmsg.Scan() {
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

		for msg.Scan() {
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

		for msg.Scan() {
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

		for msg.Scan() {
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
}

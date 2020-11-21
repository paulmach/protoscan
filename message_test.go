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

	// decode
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
				case 1:
					v, err := cmsg.Int64()
					if err != nil {
						t.Fatalf("unable to read: %v", err)
					}
					p.Child.Number = &v
				case 2:
					gcmsg, err := cmsg.Message()
					if err != nil {
						t.Fatalf("unable to read: %v", err)
					}

					gc := &testmsg.Grandchild{}
					for gcmsg.Scan() {
						switch gcmsg.FieldNumber() {
						case 1:
							v, err := gcmsg.Int64()
							if err != nil {
								t.Fatalf("unable to read: %v", err)
							}
							gc.Number = &v
						case 2:
							v, err := gcmsg.RepeatedInt64(gc.Numbers)
							if err != nil {
								t.Fatalf("unable to read: %v", err)
							}
							gc.Numbers = v
						case 32:
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
				case 3:
					v, err := cmsg.RepeatedInt64(p.Child.Numbers)
					if err != nil {
						t.Fatalf("unable to read: %v", err)
					}
					p.Child.Numbers = v
				case 32:
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

	compare(t, p, parent)
}

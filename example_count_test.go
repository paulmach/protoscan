package protoscan_test

import (
	"fmt"

	"github.com/paulmach/protoscan"
	"github.com/paulmach/protoscan/internal/testmsg"
	"google.golang.org/protobuf/proto"
)

// Count demonstrates some basics of using the library by counting elements
// in a larger customer message without fully decoding it.
func Example_count() {
	c := &testmsg.Customer{
		Id:       proto.Int64(123),
		Username: proto.String("name"),
		Orders: []*testmsg.Order{
			{
				Id:   proto.Int64(1),
				Open: proto.Bool(true),
				Items: []*testmsg.Item{
					{Id: proto.Int64(1)},
					{Id: proto.Int64(2)},
					{Id: proto.Int64(3)},
				},
			},
			{
				Id:   proto.Int64(2),
				Open: proto.Bool(false),
				Items: []*testmsg.Item{
					{Id: proto.Int64(1)},
					{Id: proto.Int64(2)},
				},
			},
			{
				Id:   proto.Int64(3),
				Open: proto.Bool(true),
				Items: []*testmsg.Item{
					{Id: proto.Int64(1)},
				},
			},
		},
		FavoriteIds: []int64{1, 2, 3, 4, 5, 6, 7, 8},
	}
	data, _ := proto.Marshal(c)

	// start the decoding
	openCount := 0
	itemCount := 0
	favoritesCount := 0

	customer := protoscan.New(data)
	for customer.Next() {
		switch customer.FieldNumber() {
		case 1: // id
			id, err := customer.Int64()
			if err != nil {
				panic(err)
			}
			_ = id // do something or skip this case if not needed

		case 2: // username
			username, err := customer.String()
			if err != nil {
				panic(err)
			}
			_ = username // do something or skip this case if not needed

		case 3: // orders
			open := false
			count := 0

			orderData, _ := customer.MessageData()
			order := protoscan.New(orderData)
			for order.Next() {
				switch order.FieldNumber() {
				case 2: // open
					v, _ := order.Bool()
					open = v
				case 3: // item
					count++

					// we're not reading the data but we still need to skip it.
					order.Skip()
				default:
					// required to move past unneeded fields
					order.Skip()
				}
			}

			if open {
				openCount++
				itemCount += count
			}
		case 4: // favorite ids
			iter, err := customer.Iterator(nil)
			if err != nil {
				panic(err)
			}

			// Typically this section would only be run once but it is valid
			// protobuf to contain multiple sections of repeated fields that should
			// be concatenated together.
			favoritesCount += iter.Count(protoscan.WireTypeVarint)
		default:
			// unread fields must be skipped
			customer.Skip()
		}
	}

	if customer.Err() != nil {
		panic(customer.Err())
	}

	fmt.Printf("Open Orders: %d\n", openCount)
	fmt.Printf("Items:       %d\n", itemCount)
	fmt.Printf("Favorites:   %d\n", favoritesCount)

	// Output:
	// Open Orders: 2
	// Items:       4
	// Favorites:   8
}

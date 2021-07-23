package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Order struct {
	OrderId    int
	CustomerId string
	Status     string
}

//consumer will subscribe ORDERS.created and publish ORDERS.approved
const (
	subSubjectName = "ORDERS.received"
	pubSubjectName = "ORDERS.processed"
)

func setup() *nats.Conn {
	natsURL := "nats://localhost:4222"

	opts := nats.Options{
		AllowReconnect: true,
		MaxReconnect:   5,
		ReconnectWait:  5 * time.Second,
		Timeout:        3 * time.Second,
		Url:            natsURL,
	}

	conn, err := opts.Connect()
	fmt.Errorf("v", err)

	return conn
}

func GetContext() nats.JetStreamContext {
	conn := setup()

	js, err := conn.JetStream()
	fmt.Errorf("v", err)

	return js
}

func main() {
	fmt.Println("SECTION: Setup Nats JS Manager")
	js := GetContext()
	recieveSubOrder(js)
	processSubOrder(js)
}

//consumer receive message "ORDERS.received"
func recieveSubOrder(js nats.JetStreamContext) {
	// Create Pull based consumer with maximum 128 inflight.
	// PullMaxWaiting defines the max inflight pull requests.
	//order review is consumer name
	sub, _ := js.PullSubscribe(subSubjectName, "NEW", nats.PullMaxWaiting(128))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		//fetch message
		msgs, _ := sub.Fetch(10, nats.Context(ctx))
		for _, msg := range msgs {
			//ack message
			msg.Ack()
			var order Order
			err := json.Unmarshal(msg.Data, &order)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("NEW consumer recived order")
			log.Printf("OrderID:%d, CustomerID: %s, Status:%s\n", order.OrderId, order.CustomerId, order.Status)
			processPubOrder(js, order)
		}
	}
}

// processOrder reviews the order and publishes ORDERS.processed event
func processPubOrder(js nats.JetStreamContext, order Order) {
	// Changing the Order status
	order.Status = "processed"
	orderJSON, _ := json.Marshal(order)
	_, err := js.Publish(pubSubjectName, orderJSON)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Order with OrderID:%d has been  %s\n", order.OrderId, order.Status)
}

//////////////////////////////
//DISPATCH consumer recieve ORDERS.processed and publish ORDERS.completed
const (
	subSubjectNameDispatch = "ORDERS.processed"
	pubSubjectNameDispatch = "ORDERS.completed"
)

//consumer DISPATCH, subsribe message "ORDERS.processed"
func processSubOrder(js nats.JetStreamContext) {
	// Create Pull based consumer with maximum 128 inflight.
	// PullMaxWaiting defines the max inflight pull requests.
	sub, _ := js.PullSubscribe(subSubjectNameDispatch, "DISPATCH", nats.PullMaxWaiting(128))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		//fetch message
		msgs, _ := sub.Fetch(10, nats.Context(ctx))
		for _, msg := range msgs {
			//ack message
			msg.Ack()
			var order Order
			err := json.Unmarshal(msg.Data, &order)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("DISPATCH consumer recived order")
			log.Printf("OrderID:%d, CustomerID: %s, Status:%s\n", order.OrderId, order.CustomerId, order.Status)
			completePubOrder(js, order)
		}
	}
}

// completePubOrder publishes ORDERS.completed event
func completePubOrder(js nats.JetStreamContext, order Order) {
	// Changing the Order status
	order.Status = "completed"
	orderJSON, _ := json.Marshal(order)
	_, err := js.Publish(pubSubjectNameDispatch, orderJSON)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Order with OrderID:%d has been  %s\n", order.OrderId, order.Status)
}

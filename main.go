package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
)

// Message ...
type Message struct {
	Mmsi        int
	MessageType string `json:"type"`
	Geometry    struct {
		GeometryType string `json:"type"`
		Coordinates  []float32
	}
}

// String ...
func (m Message) String() string {
	// return fmt.Sprintf("%v (%v years)", p.Name, p.Age)
	return fmt.Sprintf("%d of type %s", m.Mmsi, m.MessageType)
}

func mmsisToBeTracked(commaSeparatedMmsis string) (mmsis []int) {
	for _, mmsi := range strings.Split(commaSeparatedMmsis, ",") {
		number, _ := strconv.ParseInt(strings.Trim(mmsi, " "), 10, 32)
		mmsis = append(mmsis, int(number))
	}
	return
}


func messageHandler(client mqtt.Client, message mqtt.Message) {
	fmt.Printf("Received message: %s", message.Payload())
	var msg Message
	_ = json.Unmarshal(message.Payload(), &msg)
	fmt.Printf("\nReceived message: %v\n", msg)

	// checkout the message type and pass it forward to something
}

func allTopicsWithQos(mmsis []int) map[string]byte {
	topics := make(map[string]byte, 10)
	for _, mmsi := range mmsis {
		topics[fmt.Sprintf("vessels/%d/locations", mmsi)] = 0
	}
	return topics
}

func main() {
	godotenv.Load()

	fmt.Println("Creating a WebSocket client")

	opts := mqtt.NewClientOptions()
	opts.AddBroker(os.Getenv("TRAFI_URL"))
	opts.SetKeepAlive(2 * time.Second)
	opts.SetPingTimeout(time.Second)
	opts.SetUsername(os.Getenv("TRAFI_USERNAME"))
	opts.SetPassword(os.Getenv("TRAFI_PASSWORD"))
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Subscribe to all Vessels")

	vessels := mmsisToBeTracked(os.Getenv("MMSIS_TO_TRACK"))
	for topic := range allTopicsWithQos(vessels) {
		println(topic)
	}
	if token := client.SubscribeMultiple(allTopicsWithQos(vessels), messageHandler); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	exitChannel := make(chan bool, 1)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel)
	go func() {
		select {
		case <-signalChannel:
			for topic := range allTopicsWithQos(vessels) {
				client.Unsubscribe(topic)
			}
			const disconnectTimeInMs uint = 1000
			client.Disconnect(disconnectTimeInMs)
			fmt.Println("Disconnected and closed")

			exitChannel <- true
		}
	}()

	select {
	case shouldExit := <-exitChannel:
		if shouldExit {
			fmt.Println("Bye bye!")
			os.Exit(0)
		}
	}
}

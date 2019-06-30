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

type positionMessage struct {
	Mmsi        int
	MessageType string `json:"type"`
	Geometry    struct {
		GeometryType string `json:"type"`
		Coordinates  []float32
	}
	Properties struct {
		Heading   float32
		Timestamp int64 `json:"timestampExternal"`
	}
}

func vesselsToBeTracked(commaSeparatedMmsis string) (mmsis []int) {
	for _, mmsi := range strings.Split(commaSeparatedMmsis, ",") {
		number, err := strconv.ParseInt(strings.Trim(mmsi, " "), 10, 32)
		if err != nil {
			fmt.Printf("Faulty mssi detected: %s. Skipping it...\n", mmsi)
		} else {
			mmsis = append(mmsis, int(number))
		}
	}
	return
}

func allTopicsWithQos(mmsis []int) map[string]byte {
	topics := make(map[string]byte, 10)
	for _, mmsi := range mmsis {
		topics[fmt.Sprintf("vessels/%d/locations", mmsi)] = 0
	}
	return topics
}

func messageHandler(client mqtt.Client, message mqtt.Message) {
	var msg positionMessage
	err := json.Unmarshal(message.Payload(), &msg)
	if err != nil {
		fmt.Println("Failed to marshal message: ", message)
	}
	lat, lon := msg.Geometry.Coordinates[0], msg.Geometry.Coordinates[1]
	humanReadbleDate := time.Unix(msg.Properties.Timestamp, 0)
	fmt.Printf("%v - %v at (%g, %g)\n", humanReadbleDate, msg.Mmsi, lat, lon)
}

func main() {
	godotenv.Load()

	fmt.Println("Starting up...")

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

	vessels := vesselsToBeTracked(os.Getenv("VESSELS_TO_TRACK"))
	fmt.Println("Subscribe to vessels: ", vessels)

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
			fmt.Println("Unsubscribed all the topics and disconnected the client")

			exitChannel <- true
		}
	}()

	select {
	case shouldExit := <-exitChannel:
		if shouldExit {
			os.Exit(0)
		}
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

var stopInputDaemon chan struct{}
var stopOutputDaemon chan struct{}
var kafkaInputChannel chan []byte
var kafkaOutputChannel chan []byte

type Message struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

func commitToKafka() {
	topic := "wrs"
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	for {
		select {
		case <-stopInputDaemon:
			break
		case message := <-kafkaInputChannel:
			conn.WriteMessages(
				kafka.Message{Value: message},
			)
		}
	}
	conn.Close()
}

func readFromKafka() {

	for {
		topic := "wrs"
		partition := 0
		conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		batch := conn.ReadBatch(1, 1e6)

		b := make([]byte, 10e3) // 10KB max per message
		for {
			_, err := batch.Read(b)
			if err != nil {
				break
			}
			fmt.Println(string(b))
		}
		batch.Close()
		conn.Close()
		select {
		case <-stopOutputDaemon:
			break
		}
	}

}

// curl localhost:8000 -d '{"name":"Hello"}'
func Producer(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Unmarshal
	var msg Message
	err = json.Unmarshal(b, &msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	output, err := json.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	kafkaInputChannel <- output
	w.WriteHeader(http.StatusOK)
}

func main() {
	stopInputDaemon = make(chan struct{})
	stopOutputDaemon = make(chan struct{})
	kafkaInputChannel = make(chan []byte, 1024)
	kafkaOutputChannel = make(chan []byte, 1024)
	go commitToKafka()
	go readFromKafka()
	r := mux.NewRouter()
	r.HandleFunc("/", Producer).Methods("POST")
	address := ":8000"
	log.Println("Starting server on address", address)
	err := http.ListenAndServe(address, r)
	if err != nil {
		close(stopInputDaemon)
		close(stopOutputDaemon)
		panic(err)
	}
}

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Response struct {
	message_size   int32
	correlation_id int32
}

func (r *Response) serialize() []byte {
	bytes := make([]byte, 8) // Correct length for the byte slice

	binary.BigEndian.PutUint32(bytes[0:4], uint32(r.message_size))
	binary.BigEndian.PutUint32(bytes[4:8], uint32(r.correlation_id))

	return bytes
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	fmt.Println("Listening on: 9092")
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	request := make([]byte, 1024)
	n, err := conn.Read(request)

	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
	}
	if n < 12 {
		fmt.Println("Request too short")
	}

	var header RequestHeaderv2

	header.correlation_id = int32(binary.BigEndian.Uint32(request[8:12]))

	response := Response{
		message_size:   0,
		correlation_id: header.correlation_id,
	}

	responseBytes := response.serialize()

	_, err = conn.Write(responseBytes)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
	}

	conn.Close()
}

type RequestHeaderv2 struct {
	message_size        int32
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	client_id           string
}

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
	response := Response{
		message_size:   0,
		correlation_id: 7,
	}

	responseBytes := response.serialize()

	_, err := conn.Write(responseBytes)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
	}

	conn.Close()
}

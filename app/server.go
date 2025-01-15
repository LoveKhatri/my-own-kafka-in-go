package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"slices"
)

type Response struct {
	message_size   int32
	correlation_id int32
	error_code     int16
}

var supportedApiVersions = []int16{0, 1, 2, 3, 4}

func (r *Response) serialize() []byte {
	bytes := make([]byte, 10)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(r.message_size))
	binary.BigEndian.PutUint32(bytes[4:8], uint32(r.correlation_id))
	binary.BigEndian.PutUint16(bytes[8:10], uint16(r.error_code))

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

	var header Request

	header.request_api_version = int16(binary.BigEndian.Uint16(request[6:8]))
	header.correlation_id = int32(binary.BigEndian.Uint32(request[8:12]))

	var errorCode int16 = 0
	if !slices.Contains(supportedApiVersions, header.request_api_version) {
		errorCode = 35 // UNSUPPORTED_VERSION
	}

	response := Response{
		message_size:   0,
		correlation_id: header.correlation_id,
		error_code:     errorCode,
	}
	// Validate the API version

	responseBytes := response.serialize()

	_, err = conn.Write(responseBytes)
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
	}

	conn.Close()
}

type Request struct {
	message_size        int32
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	client_id           string
}

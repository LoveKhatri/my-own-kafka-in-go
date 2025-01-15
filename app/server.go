package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"slices"
)

var supportedApiVersions = []int16{0, 1, 2, 3, 4}

type Request struct {
	// message_size        int32
	// request_api_key     int16
	request_api_version int16
	correlation_id      int32
	// client_id           string
}

type Response struct {
	message_size   int32
	correlation_id int32
	error_code     int16
}

type APIResponse struct {
	num_of_api_keys  int8
	api_key          int16
	min_version      int16
	max_version      int16
	tagged_fields    byte
	throttle_time_ms int32
	tagged_fields_2  byte
}

func (r *Response) serialize() []byte {
	bytes := make([]byte, 10)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(r.message_size))
	binary.BigEndian.PutUint32(bytes[4:8], uint32(r.correlation_id))
	binary.BigEndian.PutUint16(bytes[8:10], uint16(r.error_code))

	return bytes
}

func (r *APIResponse) serialize() []byte {
	bytes := make([]byte, 13)

	bytes[0] = byte(r.num_of_api_keys)
	binary.BigEndian.PutUint16(bytes[1:3], uint16(r.api_key))
	binary.BigEndian.PutUint16(bytes[3:5], uint16(r.min_version))
	binary.BigEndian.PutUint16(bytes[5:7], uint16(r.max_version))
	bytes[7] = r.tagged_fields
	binary.BigEndian.PutUint32(bytes[8:12], uint32(r.throttle_time_ms))
	bytes[12] = r.tagged_fields_2

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
	defer conn.Close()

	for {
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
			message_size:   19,
			correlation_id: header.correlation_id,
			error_code:     errorCode,
		}

		responseBytes := response.serialize()

		_, err = conn.Write(responseBytes)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
		}

		apiResponse := APIResponse{
			num_of_api_keys:  2,
			api_key:          18,
			min_version:      0,
			max_version:      4,
			tagged_fields:    0,
			throttle_time_ms: 0,
			tagged_fields_2:  0,
		}

		apiResponseBytes := apiResponse.serialize()

		_, err = conn.Write(apiResponseBytes)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
		}
	}
}

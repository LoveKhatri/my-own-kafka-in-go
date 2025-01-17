package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type Request struct {
	message_size        int32
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	// client_id           string
}

type Response struct {
	correlation_id int32
}

type APIResponse struct {
	error_code       int16
	num_of_api_keys  int8
	apis             []APIInfo
	throttle_time_ms int32
	tagged_fields_2  byte
}

type APIInfo struct {
	api_key       int16
	min_version   int16
	max_version   int16
	tagged_fields int8
}

var supportedAPIs = map[int16]APIInfo{
	18: {18, 0, 4, 0},
	75: {75, 0, 0, 0},
}

func (r *Response) serialize(apiRes APIResponse) []byte {
	bytes := make([]byte, 8)
	apiResBytes := apiRes.serialize()
	message_size := 4 + len(apiResBytes)

	binary.BigEndian.PutUint32(bytes[0:4], uint32(message_size))
	binary.BigEndian.PutUint32(bytes[4:8], uint32(r.correlation_id))

	finalBytes := append(bytes, apiResBytes...)

	return finalBytes
}

func (r *APIResponse) serialize() []byte {
	// error_code + num_apis + (api entries) + throttle + tags
	size := 2 + 1 + (7 * len(r.apis)) + 4 + 1
	fmt.Println("Size: ", size)

	bytes := make([]byte, size)

	offset := 0
	binary.BigEndian.PutUint16(bytes[offset:], uint16(r.error_code))
	offset += 2

	bytes[offset] = byte(r.num_of_api_keys)
	offset++

	// Serialize each API info
	for _, api := range r.apis {
		binary.BigEndian.PutUint16(bytes[offset:], uint16(api.api_key))
		binary.BigEndian.PutUint16(bytes[offset+2:], uint16(api.min_version))
		binary.BigEndian.PutUint16(bytes[offset+4:], uint16(api.max_version))
		bytes[offset+6] = 0
		offset += 7
	}

	binary.BigEndian.PutUint32(bytes[offset:], uint32(r.throttle_time_ms))
	offset += 4

	bytes[offset] = r.tagged_fields_2

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
		request, err := readRequest(conn)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client closed the connection")
				return
			}
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}

		response := Response{
			correlation_id: request.correlation_id,
		}

		apiRes := generateApiResponse(request)

		responseBytes := response.serialize(apiRes)
		fmt.Println("Response: ", responseBytes)

		_, err = conn.Write(responseBytes)
		if err != nil {
			if err == net.ErrClosed || err.Error() == "write: broken pipe" {
				fmt.Println("Client closed the connection before writing response")
				return
			}
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

func readRequest(conn net.Conn) (Request, error) {
	request := make([]byte, 1024)
	n, err := conn.Read(request)
	if err != nil {
		return Request{}, err
	}

	if n < 12 {
		return Request{}, fmt.Errorf("Request too short")
	}

	var header Request

	header.message_size = int32(binary.BigEndian.Uint32(request[0:4]))
	header.request_api_key = int16(binary.BigEndian.Uint16(request[4:6]))
	header.request_api_version = int16(binary.BigEndian.Uint16(request[6:8]))
	header.correlation_id = int32(binary.BigEndian.Uint32(request[8:12]))

	return header, nil
}

func generateApiResponse(request Request) APIResponse {
	errorCode := 0

	if request.request_api_key == 18 {
		if request.request_api_version < 0 || request.request_api_version > 4 {
			errorCode = 35
		}
	}

	apis := []APIInfo{
		supportedAPIs[18],
	}

	for key, api := range supportedAPIs {
		if key != 18 {
			apis = append(apis, api)
		}
	}

	return APIResponse{
		error_code:       int16(errorCode),
		num_of_api_keys:  int8(len(apis) + 1),
		apis:             apis,
		throttle_time_ms: 0,
		tagged_fields_2:  0,
	}
}

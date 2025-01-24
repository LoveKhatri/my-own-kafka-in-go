package api

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/types"
	"github.com/google/uuid"
)

func ParseFetchV16Request(body []byte) (FetchV16Request, error) {
	var req FetchV16Request

	offset := 0

	req.MaxWaitMs = int32(binary.BigEndian.Uint32(body[offset:]))
	fmt.Println("MaxWaitMs: ", req.MaxWaitMs, " Offset: ", offset, " Body: ", body[offset:offset+4])
	offset += 4

	req.MinBytes = int32(binary.BigEndian.Uint32(body[offset:]))
	fmt.Println("MinBytes: ", req.MinBytes, " Offset: ", offset, " Body: ", body[offset:offset+4])
	offset += 4

	req.MaxBytes = int32(binary.BigEndian.Uint32(body[offset:]))
	fmt.Println("MaxBytes: ", req.MaxBytes, " Offset: ", offset, " Body: ", body[offset:offset+4])
	offset += 4

	req.IsolationLevel = int8(body[offset])
	fmt.Println("IsolationLevel: ", req.IsolationLevel, " Offset: ", offset, " Body: ", body[offset:offset+1])
	offset++

	req.SessionId = int32(binary.BigEndian.Uint32(body[offset:]))
	fmt.Println("SessionId: ", req.SessionId, " Offset: ", offset, " Body: ", body[offset:offset+4])
	offset += 4

	req.SessionEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
	fmt.Println("SessionEpoch: ", req.SessionEpoch, " Offset: ", offset, " Body: ", body[offset:offset+4])
	offset += 4

	var topics []FetchRequestTopic
	topicsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	fmt.Println("TopicsCount: ", topicsCount, " Offset: ", offset, " Body: ", body[offset:offset+numOfBytes])
	offset += numOfBytes

	for i := 0; i < int(topicsCount); i++ {
		var topic FetchRequestTopic

		topic.TopicId = uuid.UUID(body[offset : offset+16])
		fmt.Println("TopicId: ", topic.TopicId, " Offset: ", offset, " Body: ", body[offset:offset+16])
		offset += 16

		var partitions []FetchRequestTopicPartition

		partitionsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
		fmt.Println("PartitionsCount: ", partitionsCount, " Offset: ", offset, " Body: ", body[offset:offset+numOfBytes])
		offset += numOfBytes

		for j := 0; j < int(partitionsCount); j++ {
			var partition FetchRequestTopicPartition

			partition.Partition = int32(binary.BigEndian.Uint32(body[offset:]))
			fmt.Println("Partition: ", partition.Partition, " Offset: ", offset, " Body: ", body[offset:offset+4])
			offset += 4

			partition.CurrentLeaderEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
			fmt.Println("CurrentLeaderEpoch: ", partition.CurrentLeaderEpoch, " Offset: ", offset, " Body: ", body[offset:offset+4])
			offset += 4

			partition.FetchOffset = int64(binary.BigEndian.Uint64(body[offset:]))
			fmt.Println("FetchOffset: ", partition.FetchOffset, " Offset: ", offset, " Body: ", body[offset:offset+8])
			offset += 8

			partition.LastFetchedEpoch = int32(binary.BigEndian.Uint32(body[offset:]))
			fmt.Println("LastFetchedEpoch: ", partition.LastFetchedEpoch, " Offset: ", offset, " Body: ", body[offset:offset+4])
			offset += 4

			partition.LogStartOffset = int64(binary.BigEndian.Uint64(body[offset:]))
			fmt.Println("LogStartOffset: ", partition.LogStartOffset, " Offset: ", offset, " Body: ", body[offset:offset+8])
			offset += 8

			partition.PartitionMaxBytes = int32(binary.BigEndian.Uint32(body[offset:]))
			fmt.Println("PartitionMaxBytes: ", partition.PartitionMaxBytes, " Offset: ", offset, " Body: ", body[offset:offset+4])
			offset += 4

			partitions = append(partitions, partition)
		}

		topic.Partitions = partitions
		topic.TAG_BUFFER = byte(body[offset])
		fmt.Println("TAG_BUFFER: ", topic.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset:offset+1])
		offset++

		topics = append(topics, topic)
	}

	req.Topics = topics

	var forgottenTopicsData []FetchRequestForgottenTopicsData

	forgottenTopicsDataCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	fmt.Println("ForgottenTopicsDataCount: ", forgottenTopicsDataCount, " Offset: ", offset, " Body: ", body[offset:offset+numOfBytes])
	offset += numOfBytes

	for i := 0; i < int(forgottenTopicsDataCount); i++ {
		var forgottenTopicData FetchRequestForgottenTopicsData

		forgottenTopicData.TopicId = uuid.UUID(body[offset : offset+16])
		fmt.Println("ForgottenTopicData.TopicId: ", forgottenTopicData.TopicId, " Offset: ", offset, " Body: ", body[offset:offset+16])
		offset += 16

		var partitions []int32

		partitionsCount, numOfBytes, _ := DecodeSignedVarint(body[offset:])
		fmt.Println("ForgottenTopicData.PartitionsCount: ", partitionsCount, " Offset: ", offset, " Body: ", body[offset:offset+numOfBytes])
		offset += numOfBytes

		for j := 0; j < int(partitionsCount); j++ {
			partition := int32(binary.BigEndian.Uint32(body[offset:]))
			fmt.Println("Partition: ", partition, " Offset: ", offset, " Body: ", body[offset:offset+4])
			offset += 4

			partitions = append(partitions, partition)
		}

		forgottenTopicData.Partitions = partitions
		forgottenTopicData.TAG_BUFFER = byte(body[offset])
		fmt.Println("ForgottenTopicData.TAG_BUFFER: ", forgottenTopicData.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset:offset+1])
		offset++

		forgottenTopicsData = append(forgottenTopicsData, forgottenTopicData)
	}

	req.ForgottenTopicsData = forgottenTopicsData

	rackIdLength, numOfBytes, _ := DecodeSignedVarint(body[offset:])
	fmt.Println("RackIdLength: ", rackIdLength, " Offset: ", offset, " Body: ", body[offset:offset+numOfBytes])
	offset += numOfBytes

	if rackIdLength > 0 {
		req.RackId = string(body[offset : offset+int(rackIdLength-1)])
		fmt.Println("RackId: ", req.RackId, " Offset: ", offset, " Body: ", body[offset:])
		offset += int(rackIdLength - 1)
	}

	req.TAG_BUFFER = byte(body[offset])
	fmt.Println("TAG_BUFFER: ", req.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset:offset+1])
	offset++

	return req, nil
}

func GenerateFetchResponse(request types.RequestMessage) ([]byte, error) {
	req := request.Body.(FetchV16Request)

	var res FetchV16Response

	res.ThrottleTimeMs = 0
	res.ErrorCode = 0
	res.SessionId = req.SessionId

	var responses []FetchResponseTopic

	for _, topic := range req.Topics {
		var topicRes FetchResponseTopic

		topicRes.TopicId = topic.TopicId

		var partitions []FetchResponseTopicPartitions

		for _, partition := range topic.Partitions {
			var partitionRes FetchResponseTopicPartitions

			partitionRes.PartitionIndex = partition.Partition
			partitionRes.ErrorCode = 0
			partitionRes.HighWatermark = 0
			partitionRes.LastStableOffset = 0
			partitionRes.LogStartOffset = 0
			partitionRes.AbortedTransactions = []FetchResponseAbortedTransactions{}
			partitionRes.PreferredReadReplica = -1
			partitionRes.Records = []byte{}

			partitions = append(partitions, partitionRes)
		}

		topicRes.Partitions = partitions
		topicRes.TAG_BUFFER = 0

		responses = append(responses, topicRes)
	}

	res.Responses = responses
	res.TAG_BUFFER = 0

	fmt.Printf("Fetch Response: %+v\n", res)

	bytes := res.Serialize()

	return bytes, nil
}

func (req FetchV16Response) Serialize() []byte {
	body := make([]byte, 1024)

	offset := 0

	body[offset] = 0
	offset++

	binary.BigEndian.PutUint32(body[offset:], uint32(req.ThrottleTimeMs))
	offset += 4
	fmt.Println("ThrottleTimeMs: ", req.ThrottleTimeMs, " Offset: ", offset, " Body: ", body[offset-4:offset])

	binary.BigEndian.PutUint16(body[offset:], uint16(req.ErrorCode))
	offset += 2
	fmt.Println("ErrorCode: ", req.ErrorCode, " Offset: ", offset, " Body: ", body[offset-2:offset])

	binary.BigEndian.PutUint32(body[offset:], uint32(req.SessionId))
	offset += 4
	fmt.Println("SessionId: ", req.SessionId, " Offset: ", offset, " Body: ", body[offset-4:offset])

	responsesLength := EncodeSignedVarint(len(req.Responses))
	copy(body[offset:], responsesLength)
	offset += len(responsesLength)
	fmt.Println("ResponsesLength: ", len(req.Responses), " Offset: ", offset, " Body: ", body[offset-len(responsesLength):offset])

	for _, topic := range req.Responses {
		copy(body[offset:], topic.TopicId[:])
		offset += 16
		fmt.Println("TopicId: ", topic.TopicId, " Offset: ", offset, " Body: ", body[offset-16:offset])

		partitionsLength := EncodeSignedVarint(len(topic.Partitions))
		copy(body[offset:], partitionsLength)
		offset += len(partitionsLength)
		fmt.Println("PartitionsLength: ", len(topic.Partitions), " Offset: ", offset, " Body: ", body[offset-len(partitionsLength):offset])

		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint32(body[offset:], uint32(partition.PartitionIndex))
			offset += 4
			fmt.Println("PartitionIndex: ", partition.PartitionIndex, " Offset: ", offset, " Body: ", body[offset-4:offset])

			binary.BigEndian.PutUint16(body[offset:], uint16(partition.ErrorCode))
			offset += 2
			fmt.Println("ErrorCode: ", partition.ErrorCode, " Offset: ", offset, " Body: ", body[offset-2:offset])

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.HighWatermark))
			offset += 8
			fmt.Println("HighWatermark: ", partition.HighWatermark, " Offset: ", offset, " Body: ", body[offset-8:offset])

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.LastStableOffset))
			offset += 8
			fmt.Println("LastStableOffset: ", partition.LastStableOffset, " Offset: ", offset, " Body: ", body[offset-8:offset])

			binary.BigEndian.PutUint64(body[offset:], uint64(partition.LogStartOffset))
			offset += 8
			fmt.Println("LogStartOffset: ", partition.LogStartOffset, " Offset: ", offset, " Body: ", body[offset-8:offset])

			abortedTransactionsLength := EncodeSignedVarint(len(partition.AbortedTransactions))
			copy(body[offset:], abortedTransactionsLength)
			offset += len(abortedTransactionsLength)
			fmt.Println("AbortedTransactionsLength: ", len(partition.AbortedTransactions), " Offset: ", offset, " Body: ", body[offset-len(abortedTransactionsLength):offset])

			for _, transaction := range partition.AbortedTransactions {
				binary.BigEndian.PutUint64(body[offset:], uint64(transaction.ProducerId))
				offset += 8
				fmt.Println("ProducerId: ", transaction.ProducerId, " Offset: ", offset, " Body: ", body[offset-8:offset])

				binary.BigEndian.PutUint64(body[offset:], uint64(transaction.FirstOffset))
				offset += 8
				fmt.Println("FirstOffset: ", transaction.FirstOffset, " Offset: ", offset, " Body: ", body[offset-8:offset])

				body[offset] = transaction.TAG_BUFFER
				offset++
				fmt.Println("TAG_BUFFER: ", transaction.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset-1:offset])
			}

			binary.BigEndian.PutUint32(body[offset:], uint32(partition.PreferredReadReplica))
			offset += 4
			fmt.Println("PreferredReadReplica: ", partition.PreferredReadReplica, " Offset: ", offset, " Body: ", body[offset-4:offset])

			copy(body[offset:], partition.Records)
			offset += len(partition.Records)
			fmt.Println("Records: ", partition.Records, " Offset: ", offset, " Body: ", body[offset-len(partition.Records):offset])

			body[offset] = partition.TAG_BUFFER
			offset++
			fmt.Println("TAG_BUFFER: ", partition.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset-1:offset])
		}

		body[offset] = topic.TAG_BUFFER
		offset++
		fmt.Println("TAG_BUFFER: ", topic.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset-1:offset])
	}

	body[offset] = req.TAG_BUFFER
	offset++
	fmt.Println("TAG_BUFFER: ", req.TAG_BUFFER, " Offset: ", offset, " Body: ", body[offset-1:offset])

	return body[:offset]
}

type FetchV16Request struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionId           int32
	SessionEpoch        int32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopicsData
	RackId              string
	TAG_BUFFER          byte
}

type FetchRequestTopic struct {
	TopicId    uuid.UUID
	Partitions []FetchRequestTopicPartition
	TAG_BUFFER byte
}

type FetchRequestTopicPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	TAG_BUFFER         byte
}

type FetchRequestForgottenTopicsData struct {
	TopicId    uuid.UUID
	Partitions []int32
	TAG_BUFFER byte
}

type FetchV16Response struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Responses      []FetchResponseTopic
	TAG_BUFFER     byte
}

type FetchResponseTopic struct {
	TopicId    uuid.UUID
	Partitions []FetchResponseTopicPartitions
	TAG_BUFFER byte
}

type FetchResponseTopicPartitions struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []FetchResponseAbortedTransactions
	PreferredReadReplica int32
	Records              []byte
	TAG_BUFFER           byte
}

type FetchResponseAbortedTransactions struct {
	ProducerId  int64
	FirstOffset int64
	TAG_BUFFER  byte
}

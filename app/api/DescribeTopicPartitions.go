package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/types"
	"github.com/google/uuid"
)

type DescribeTopicPartitionsv0Request struct {
	Topics                 []topics
	ResponsePartitionLimit int32
	Cursor                 cursor
	TAG_BUFFER             byte
}
type topics struct {
	Name       types.CompactString
	TAG_BUFFER byte
}
type cursor struct {
	TopicName      types.CompactString
	PartitionIndex int32
	TAG_BUFFER     byte
}

type DescribeTopicPartitionsv0Response struct {
	ThrottleTimeMs int32
	Topics         types.CompactArray[ResponseTopic]
	NextCursor     NextCursor
	TAG_BUFFER     byte
}

type ResponseTopic struct {
	ErrorCode                 int16
	Name                      types.CompactNullableString
	TopicId                   uuid.UUID
	IsInternal                bool
	Partitions                types.CompactArray[ResponsePartition]
	TopicAuthorizedOperations int32
	TAG_BUFFER                byte
}

type ResponsePartition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           types.CompactArray[int32]
	IsrNodes               types.CompactArray[int32]
	EligibleLeaderReplicas types.CompactArray[int32]
	LastKnownElr           types.CompactArray[int32]
	OfflineReplicas        types.CompactArray[int32]
	TAG_BUFFER             byte
}

type NextCursor struct {
	TopicName      types.CompactString
	PartitionIndex int32
}

func ParseDescribeTopicPartitionsV0Request(data []byte) (DescribeTopicPartitionsv0Request, error) {
	var body DescribeTopicPartitionsv0Request

	offset := 0

	topicsCount := int8(data[offset])
	offset++

	var allTopics []topics
	for i := 0; i < int(topicsCount-1); i++ {
		topicNameLength := int8(data[offset])
		offset++

		topicName := string(data[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength - 1)

		topicTagBuffer := data[offset]
		offset++

		allTopics = append(allTopics, topics{
			Name:       types.CompactString(topicName),
			TAG_BUFFER: topicTagBuffer,
		})
	}

	body.Topics = allTopics

	body.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// If next byte is 0xff, then there is no cursor meaning null
	var cursorData cursor

	nextByte := data[offset]
	offset++

	if nextByte != 0xff {
		topicNameLength := int8(data[offset])
		offset++

		topicName := string(data[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

		partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		cursorData = cursor{
			TopicName:      types.CompactString(topicName),
			PartitionIndex: partitionIndex,
			TAG_BUFFER:     data[offset],
		}
	}

	body.Cursor = cursorData

	body.TAG_BUFFER = data[offset]
	offset++

	return body, nil
}

func GenerateDescribeTopicPartitionsV0Response(request types.RequestMessage) ([]byte, error) {
	body := request.Body.(DescribeTopicPartitionsv0Request)

	topic := ResponseTopic{
		ErrorCode: 3,
		Name:      types.CompactNullableString(body.Topics[0].Name),
		// TopicId:   uuid.MustParse("99999999-9999-9999-9999-999999999999"),
		TopicId:                   uuid.MustParse("00000000-0000-0000-0000-000000000000"),
		IsInternal:                false,
		Partitions:                types.CompactArray[ResponsePartition]{},
		TopicAuthorizedOperations: 3576,
		TAG_BUFFER:                0,
	}

	res := DescribeTopicPartitionsv0Response{
		ThrottleTimeMs: 0,
		Topics:         types.CompactArray[ResponseTopic]{topic},
		NextCursor:     NextCursor{},
		TAG_BUFFER:     0,
	}

	resBytes := res.Serialize()

	return resBytes, nil
}

func (r *DescribeTopicPartitionsv0Response) Serialize() []byte {
	resBytes := make([]byte, 1024) // Adjust the size as needed

	offset := 0

	resBytes[offset] = 0
	offset++

	binary.BigEndian.PutUint32(resBytes[offset:], uint32(r.ThrottleTimeMs))
	offset += 4

	topicsLength := len(r.Topics) + 1
	resBytes[offset] = byte(topicsLength)
	offset++

	for _, topic := range r.Topics {
		binary.BigEndian.PutUint16(resBytes[offset:], uint16(topic.ErrorCode))
		offset += 2

		topicNameBytes, _ := topic.Name.Serialize()
		copy(resBytes[offset:], topicNameBytes)
		offset += len(topicNameBytes) - 1

		topicIdBytes := topic.TopicId[:]
		copy(resBytes[offset:], topicIdBytes)
		offset += 16

		if topic.IsInternal {
			resBytes[offset] = 1
		} else {
			resBytes[offset] = 0
		}
		offset++

		// Partitions Length byte
		resBytes[offset] = byte(len(topic.Partitions) + 1)
		offset++

		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint16(resBytes[offset:], uint16(partition.ErrorCode))
			offset += 2
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.PartitionIndex))
			offset += 4
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.LeaderId))
			offset += 4
			binary.BigEndian.PutUint32(resBytes[offset:], uint32(partition.LeaderEpoch))
			offset += 4

			resBytes[offset] = byte(len(partition.ReplicaNodes))
			offset++
			for _, node := range partition.ReplicaNodes {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.IsrNodes))
			offset++
			for _, node := range partition.IsrNodes {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.EligibleLeaderReplicas))
			offset++
			for _, node := range partition.EligibleLeaderReplicas {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.LastKnownElr))
			offset++
			for _, node := range partition.LastKnownElr {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = byte(len(partition.OfflineReplicas))
			offset++
			for _, node := range partition.OfflineReplicas {
				binary.BigEndian.PutUint32(resBytes[offset:], uint32(node))
				offset += 4
			}

			resBytes[offset] = partition.TAG_BUFFER
			offset++
		}

		binary.BigEndian.PutUint32(resBytes[offset:], uint32(topic.TopicAuthorizedOperations))
		offset += 4

		resBytes[offset] = topic.TAG_BUFFER
		offset++
	}

	if r.NextCursor.TopicName == "" {
		resBytes[offset] = 0xff
		offset++
	} else {
		nextCursorTopicNameBytes, _ := r.NextCursor.TopicName.Serialize()

		copy(resBytes[offset:], nextCursorTopicNameBytes)
		offset += len(nextCursorTopicNameBytes)

		binary.BigEndian.PutUint32(resBytes[offset:], uint32(r.NextCursor.PartitionIndex))
		offset += 4
	}

	resBytes[offset] = r.TAG_BUFFER
	offset++

	return resBytes[:offset]
}

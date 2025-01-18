package types

import (
	"encoding/binary"
)

// CompactString represents a non-null sequence of characters whose length is encoded as UNSIGNED_VARINT.
type CompactString string

// type CompactString struct {
// 	Value *string
// 	Data  []byte
// }

// Serialize serializes the CompactString to a byte slice.
func (cs CompactString) Serialize() ([]byte, error) {
	actualLength := len(cs)
	if actualLength == 0 {
		return []byte{1}, nil
	}

	length := actualLength + 1

	data := []byte{byte(length)}

	return append(data, []byte(cs)...), nil
}

// // Deserialize deserializes the internal Data field to populate the Value.
// func (cs *CompactString) Deserialize() error {
// 	if len(cs.Data) < 1 {
// 		return errors.New("data too short to contain CompactString length")
// 	}

// 	length := int(cs.Data[0]) - 1 // Subtract 1 to get actual string size
// 	if length < 0 {
// 		return errors.New("invalid length for CompactString")
// 	}
// 	if len(cs.Data) < 1+length {
// 		return errors.New("data too short to contain CompactString")
// 	}

// 	str := string(cs.Data[1 : 1+length])
// 	cs.Value = &str
// 	return nil
// }

type NullableString string

// type NullableString struct {
// 	Length int16
// 	Value  *string
// 	Data   []byte
// }

// Serialize serializes the NullableString to a byte slice.
func (ns *NullableString) Serialize() ([]byte, error) {
	if ns == nil {
		// Null => length is -1
		return []byte{0xFF, 0xFF}, nil
	}

	strBytes := []byte(*ns)
	actualLength := len(strBytes)
	length := int16(actualLength)

	// Store length as 2 bytes
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, uint16(length))

	return append(data, strBytes...), nil
}

// // Deserialize deserializes the internal Data field to populate the Value.
// func (ns *NullableString) Deserialize() error {
// 	if len(ns.Data) < 2 {
// 		return errors.New("data too short to contain NullableString length")
// 	}

// 	length := int16(binary.BigEndian.Uint16(ns.Data[:2]))
// 	ns.Length = length
// 	if length == -1 {
// 		ns.Value = nil
// 		return nil // Null value
// 	}

// 	if len(ns.Data) < 2+int(length) {
// 		return errors.New("data too short to contain NullableString")
// 	}

// 	str := string(ns.Data[2 : 2+length])
// 	ns.Value = &str
// 	return nil
// }

// CompactNullableString represents a sequence of characters or null.
// The length N + 1 is given as an UNSIGNED_VARINT (stored in a byte here for simplicity).
// A null string is represented with a length of 0.
type CompactNullableString string

// type CompactNullableString struct {
// 	Value *string
// 	Data  []byte
// }

// Serialize serializes the CompactNullableString to a byte slice.
func (cns *CompactNullableString) Serialize() ([]byte, error) {
	if cns == nil {
		// Null => length is 0
		return []byte{0}, nil
	}

	strBytes := []byte(*cns)
	actualLength := len(strBytes)
	length := actualLength // N + 1 for UNSIGNED_VARINT

	// Store length in a single byte (assuming < 256)
	data := []byte{byte(length)}

	return append(data, strBytes...), nil
}

// // Deserialize deserializes the internal Data field to populate the Value.
// func (cns *CompactNullableString) Deserialize() error {
// 	if len(cns.Data) < 1 {
// 		return errors.New("data too short to contain CompactNullableString length")
// 	}

// 	length := int(cns.Data[0])
// 	if length == 0 {
// 		// Null value
// 		cns.Value = nil
// 		return nil
// 	}

// 	length--
// 	if len(cns.Data) < 1+length {
// 		return errors.New("data too short to contain CompactNullableString")
// 	}

// 	str := string(cns.Data[1 : 1+length])
// 	cns.Value = &str
// 	return nil
// }

// Array represents an array of any elements.
// If Elements is nil, it is considered a null array (length = -1).
type Array[T any] struct {
	Elements []T
}

// func (arr *Array[T]) Serialize() ([]byte, error) {
// 	// If nil, length = -1 (which is 0xFFFFFFFF in two's complement)
// 	var length int32
// 	if arr.Elements == nil {
// 		length = -1
// 	} else {
// 		length = int32(len(arr.Elements))
// 	}

// 	buf := make([]byte, 4)
// 	binary.BigEndian.PutUint32(buf, uint32(length))

// 	if length > 0 {
// 		for _, elem := range arr.Elements {
// 			serializedElem, err := elem.Serialize()
// 			if err != nil {
// 				return nil, err
// 			}
// 			buf = append(buf, serializedElem...)
// 		}
// 	}

// 	return buf, nil
// }

// func (arr *Array[T]) Deserialize(r io.Reader, newElem func() T) error {
// 	// Read length as int32
// 	lengthBuf := make([]byte, 4)
// 	if _, err := io.ReadFull(r, lengthBuf); err != nil {
// 		return err
// 	}
// 	length := int32(binary.BigEndian.Uint32(lengthBuf))

// 	// Null array
// 	if length == -1 {
// 		arr.Elements = nil
// 		return nil
// 	}

// 	arr.Elements = make([]T, 0, length)
// 	for i := int32(0); i < length; i++ {
// 		elem := newElem()
// 		if err := elem.Deserialize(r); err != nil {
// 			return err
// 		}
// 		arr.Elements = append(arr.Elements, elem)
// 	}
// 	return nil
// }

// CompactArray represents a compact array of any elements.
// If the length read via UnsignedVarInt is 0, it is considered a null array.
type CompactArray[T any] []T

// func (cArr *CompactArray[T]) Serialize() ([]byte, error) {
// 	// If nil, length = 0 => null
// 	// else length = N + 1
// 	var length uint64
// 	if cArr == nil {
// 		length = 0
// 	} else {
// 		length = uint64(len(*cArr)) + 1
// 	}

// 	// Serialize the UnsignedVarInt for length
// 	lengthBuf := encodeUnsignedVarInt(length)
// 	buf := append([]byte{}, lengthBuf...)

// 	// If length > 1, we have actual elements
// 	if length > 1 {
// 		for _, elem := range *cArr {
// 			serializedElem, err := elem.Serialize()
// 			if err != nil {
// 				return nil, err
// 			}
// 			buf = append(buf, serializedElem...)
// 		}

// 	}
// 	return buf, nil
// }

// func (cArr *CompactArray[T]) Deserialize(r io.Reader, newElem func() T) error {
// 	length, err := decodeUnsignedVarInt(r)
// 	if err != nil {
// 		return err
// 	}
// 	// If length == 0 => null array; else array of length-1 elements
// 	if length == 0 {
// 		cArr.Elements = nil
// 		return nil
// 	}

// 	n := length - 1
// 	cArr.Elements = make([]T, 0, n)
// 	for i := uint64(0); i < n; i++ {
// 		elem := newElem()
// 		if err := elem.Deserialize(r); err != nil {
// 			return err
// 		}
// 		cArr.Elements = append(cArr.Elements, elem)
// 	}
// 	return nil
// }

// encodeUnsignedVarInt encodes x as an unsigned varint.
// func encodeUnsignedVarInt(x uint64) []byte {
// 	var buf []byte
// 	for {
// 		if x < 128 {
// 			return append(buf, byte(x))
// 		}
// 		buf = append(buf, byte((x&127)|128))
// 		x >>= 7
// 	}
// }

// // decodeUnsignedVarInt decodes an unsigned varint from r.
// func decodeUnsignedVarInt(r io.Reader) (uint64, error) {
// 	var x uint64
// 	var s uint
// 	for {
// 		b := make([]byte, 1)
// 		if _, err := r.Read(b); err != nil {
// 			return 0, err
// 		}
// 		if b[0] < 128 {
// 			x |= uint64(b[0]) << s
// 			return x, nil
// 		}
// 		x |= uint64(b[0]&127) << s
// 		s += 7
// 		if s > 63 {
// 			return 0, errors.New("varint overflow")
// 		}
// 	}
// }

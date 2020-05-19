package bson

import (
	"encoding/hex"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Zeroer allows custom struct types to implement a report of zero
// state. All struct types that don't implement Zeroer or where IsZero
// returns false are considered to be not zero.
type Zeroer interface {
	IsZero() bool
}

var NilObjectID = primitive.NilObjectID

// D represents a BSON Document. This type can be used to represent BSON in a concise and readable
// manner. It should generally be used when serializing to BSON. For deserializing, the Raw or
// Document types should be used.
//
// Example usage:
//
// 		bson.D{{"foo", "bar"}, {"hello", "world"}, {"pi", 3.14159}}
//
// This type should be used in situations where order matters, such as MongoDB commands. If the
// order is not important, a map is more comfortable and concise.
type D = primitive.D
type RegEx = primitive.Regex

// E represents a BSON element for a D. It is usually used inside a D.
type E = bson.E

// M is an unordered, concise representation of a BSON Document. It should generally be used to
// serialize BSON when the order of the elements of a BSON document do not matter. If the element
// order matters, use a D instead.
//
// Example usage:
//
// 		bson.M{"foo": "bar", "hello": "world", "pi": 3.14159}
//
// This type is handled in the encoders as a regular map[string]interface{}. The elements will be
// serialized in an undefined, random order, and the order will be different each time.
type M = bson.M

// An A represents a BSON array. This type can be used to represent a BSON array in a concise and
// readable manner. It should generally be used when serializing to BSON. For deserializing, the
// RawArray or Array types should be used.
//
// Example usage:
//
// 		bson.A{"bar", "world", 3.14159, bson.D{{"qux", 12345}}}
//
type A = bson.A

// ObjectID primitive.ObjectID
type ObjectId = primitive.ObjectID

var ErrInvalidHex = primitive.ErrInvalidHex

// NewObjectID primitive.NewObjectID
var NewObjectId = primitive.NewObjectID

// ObjectIDHex primitive.ObjectIDFromHex
func ObjectIDFromHex(s string) (ObjectId, error) {
	obj, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return obj, ErrInvalidHex
	}
	return obj, nil
}

// ObjectIdHex returns an ObjectId from the provided hex representation.
// Calling this function with an invalid hex representation will
// cause a runtime panic. See the IsObjectIdHex function.
func ObjectIdHex(s string) ObjectId {
	d, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		panic(fmt.Sprintf("invalid input to ObjectIdHex: %q", s))
	}
	return d
}

// Marshal bson.Marshal
var Marshal = bson.Marshal
var Unmarshal = bson.Unmarshal

// IsObjectIDHex returns whether s is a valid hex representation of
// an ObjectId. See the ObjectIdHex function.
func IsObjectIdHex(s string) bool {
	if len(s) != 24 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

type Raw = bson.RawValue

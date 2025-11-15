package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"time"
)

// GenMsgHashSum generates SHA256 hash of the given data
func GenMsgHashSum(data []byte) ([]byte, error) {
	msgHash := sha256.New()
	_, err := msgHash.Write(data)
	if err != nil {
		return nil, err
	}
	return msgHash.Sum(nil), nil
}

// Encode encodes the data into bytes.
// Data can be of any type.
func Encode(data interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes bytes into the data.
// Data should be passed in the format of a pointer to a type.
func Decode(s []byte, data interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(s))
	if err := dec.Decode(data); err != nil {
		return err
	}
	return nil
}

// GetHash returns the hash of the encoded data
func GetHash(data interface{}) ([]byte, error) {
	encodedData, err := Encode(data)
	if err != nil {
		return nil, err
	}
	return GenMsgHashSum(encodedData)
}

// GetHashAsString returns the hash of the encoded data as a hex string
func GetHashAsString(data interface{}) (string, error) {
	hash, err := GetHash(data)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash), nil
}

// GenerateTX generates a transaction with s bytes
func GenerateTX(s int) []byte {
	var trans []byte
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < s; i++ {
		trans = append(trans, byte(rand.Intn(200)))
	}
	return trans
}

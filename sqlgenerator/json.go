package sqlgenerator

import (
	"encoding/json"
	"fmt"
	"math/rand"
)

// supported data types
const (
	dtSignedInt   = "SIGNED"
	dtUnsignedInt = "UNSIGNED"
	dtChar        = "CHAR(64)"
	dtBinary      = "binary(64)"
	dtDate        = "date"
	dtDateTime    = "datetime"
	dtTime        = "time"
	dtDouble      = "double"
)

func wrapString(s string) string {
	return fmt.Sprintf("\"%s\"", s)
}

func RandomValueWithType(dataType string) string {
	var val interface{}
	switch dataType {
	case dtSignedInt:
		val = Int63nWithSpecialValue(-9223372036854775808, 9223372036854775807)
	case dtUnsignedInt:
		val = Int63nWithSpecialValueUBig()
	case dtChar:
		val = wrapString(randomString(64))
	case dtBinary:
		val = wrapString(randomString(64))
	case dtDate:
		val = wrapString(randomDate())
	case dtDateTime:
		val = wrapString(randomDateTime())
	case dtTime:
		val = wrapString(randomTime())
	case dtDouble:
		val = rand.Float64()
	default:
		panic(fmt.Sprintf("unsupported data type, %v", dataType))
	}
	return fmt.Sprintf("%v", val)
}

// randomArrayJSON returns a JSON array with random values of the given data types.
func randomArrayJSON(size int, dataType string) (string, error) {
	data := make([]interface{}, size)

	for i := range data {
		var val interface{}

		switch dataType {
		case dtSignedInt:
			val = Int63nWithSpecialValue(-9223372036854775808, 9223372036854775807)
		case dtUnsignedInt:
			val = Int63nWithSpecialValueUBig()
		case dtChar:
			val = randomString(64)
		case dtBinary:
			val = randomString(64)
		case dtDate:
			val = randomDate()
		case dtDateTime:
			val = randomDateTime()
		case dtTime:
			val = randomTime()
		case dtDouble:
			val = rand.Float64()
		default:
			return "", fmt.Errorf("unsupported data type")
		}

		data[i] = val
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

// randomString returns a random string of the given length.
func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, length)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

// randomBinary returns a random binary string of the given length.
func randomBinary(length int) []byte {
	b := make([]byte, length)
	rand.Read(b)

	return b
}

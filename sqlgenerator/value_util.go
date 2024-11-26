package sqlgenerator

import (
	"fmt"
	"math/rand"
	"time"
)

var asciiRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789~!@#$%^&*()_+=-")

// random choose from slice
func choose[T any](slice []T) T {
	return slice[rand.Intn(len(slice))]
}

func randomCNChar() rune {
	lower, upper := int('\u4e00'), int('\u9fff')
	return rune(lower + rand.Intn(upper-lower))
}

func randomStringRunes(n int, mixCNChar bool) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = choose(asciiRunes)
		if mixCNChar && rand.Intn(3) == 0 {
			b[i] = randomCNChar()
		}
	}
	return string(b)
}

const (
	yearFormat     = "2006"
	dateFormat     = "2006-01-01"
	timeFormat     = "11:11:11.00"
	dateTimeFormat = "2006-01-02 15:04:05"
)

func randomGoTime() time.Time {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	return time.Unix(rand.Int63n(max-min)+min, 0)
}

func randomTimes(count int, format string) []string {
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("'%s'", randomGoTime().Format(format))
	}
	return result
}

func randomYear() string {
	return randomGoTime().Format(yearFormat)
}

func RandomYears(count int) []string {
	return randomTimes(count, yearFormat)
}

func randomDate() string {
	return randomGoTime().Format(dateFormat)
}

func RandomDates(count int) []string {
	return randomTimes(count, dateFormat)
}

func randomTime() string {
	return randomGoTime().Format(timeFormat)
}

func RandomTimes(count int) []string {
	return randomTimes(count, timeFormat)
}

func randomDateTime() string {
	return randomGoTime().Format(dateTimeFormat)
}

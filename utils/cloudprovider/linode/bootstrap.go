package linode

import (
	"crypto/rand"
	"strings"
)

const randomPasswordAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%^&*_-+=."

func GenerateRandomPassword(length int) (string, error) {
	if length < 12 {
		length = 12
	}

	buffer := make([]byte, length)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}

	var builder strings.Builder
	builder.Grow(length)
	for _, b := range buffer {
		builder.WriteByte(randomPasswordAlphabet[int(b)%len(randomPasswordAlphabet)])
	}
	return builder.String(), nil
}

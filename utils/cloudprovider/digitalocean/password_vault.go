package digitalocean

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const DropletPasswordVaultKeyEnv = "KOMARI_CLOUD_SECRET_KEY"

var (
	ErrDropletPasswordVaultDisabled = errors.New("saved root password storage is disabled; set KOMARI_CLOUD_SECRET_KEY on the server")
	ErrDropletPasswordDecryptFailed = errors.New("saved root password could not be decrypted; verify KOMARI_CLOUD_SECRET_KEY")
	ErrSavedDropletPasswordNotFound = errors.New("saved root password was not found for this droplet")
)

func IsDropletPasswordVaultEnabled() bool {
	return strings.TrimSpace(os.Getenv(DropletPasswordVaultKeyEnv)) != ""
}

func encryptDropletPassword(plaintext string) (string, error) {
	aead, err := newDropletPasswordCipher()
	if err != nil {
		return "", err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	sealed := aead.Seal(nil, nonce, []byte(plaintext), nil)
	payload := append(nonce, sealed...)
	return base64.StdEncoding.EncodeToString(payload), nil
}

func decryptDropletPassword(ciphertext string) (string, error) {
	aead, err := newDropletPasswordCipher()
	if err != nil {
		return "", err
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimSpace(ciphertext))
	if err != nil {
		return "", fmt.Errorf("%w: invalid ciphertext", ErrDropletPasswordDecryptFailed)
	}

	if len(payload) < aead.NonceSize() {
		return "", fmt.Errorf("%w: invalid ciphertext length", ErrDropletPasswordDecryptFailed)
	}

	nonce := payload[:aead.NonceSize()]
	sealed := payload[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrDropletPasswordDecryptFailed, err)
	}

	return string(plaintext), nil
}

func newDropletPasswordCipher() (cipher.AEAD, error) {
	secret := strings.TrimSpace(os.Getenv(DropletPasswordVaultKeyEnv))
	if secret == "" {
		return nil, ErrDropletPasswordVaultDisabled
	}

	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

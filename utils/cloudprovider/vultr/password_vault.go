package vultr

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/komari-monitor/komari/utils/cloudprovider/passwordvault"
)

const (
	RootPasswordVaultKeyEnv  = passwordvault.SecretKeyEnv
	RootPasswordVaultFileEnv = passwordvault.SecretKeyFileEnv
)

var (
	ErrRootPasswordVaultDisabled = errors.New("saved root password storage is unavailable; allow Komari to write ./data/cloud_secret.key or set KOMARI_CLOUD_SECRET_KEY")
	ErrRootPasswordDecryptFailed = errors.New("saved root password could not be decrypted; verify KOMARI_CLOUD_SECRET_KEY or ./data/cloud_secret.key")
	ErrSavedRootPasswordNotFound = errors.New("saved root password was not found for this instance")
)

func IsRootPasswordVaultEnabled() bool {
	return passwordvault.IsAvailable()
}

func encryptRootPassword(plaintext string) (string, error) {
	aead, err := newRootPasswordCipher()
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

func decryptRootPassword(ciphertext string) (string, error) {
	aead, err := newRootPasswordCipher()
	if err != nil {
		return "", err
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimSpace(ciphertext))
	if err != nil {
		return "", fmt.Errorf("%w: invalid ciphertext", ErrRootPasswordDecryptFailed)
	}

	if len(payload) < aead.NonceSize() {
		return "", fmt.Errorf("%w: invalid ciphertext length", ErrRootPasswordDecryptFailed)
	}

	nonce := payload[:aead.NonceSize()]
	sealed := payload[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrRootPasswordDecryptFailed, err)
	}

	return string(plaintext), nil
}

func newRootPasswordCipher() (cipher.AEAD, error) {
	secret, err := passwordvault.LoadOrCreateSecret()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRootPasswordVaultDisabled, err)
	}

	key := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

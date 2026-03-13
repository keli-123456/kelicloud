package passwordvault

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	SecretKeyEnv  = "KOMARI_CLOUD_SECRET_KEY"
	secretKeyPath = "./data/cloud_secret.key"
)

func IsAvailable() bool {
	_, err := LoadOrCreateSecret()
	return err == nil
}

func LoadOrCreateSecret() (string, error) {
	if secret := strings.TrimSpace(os.Getenv(SecretKeyEnv)); secret != "" {
		return secret, nil
	}

	if secret, err := readSecretFromFile(); err == nil {
		return secret, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	if err := os.MkdirAll(filepath.Dir(secretKeyPath), 0o700); err != nil {
		return "", err
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	secret := base64.RawURLEncoding.EncodeToString(buf)
	if err := os.WriteFile(secretKeyPath, []byte(secret+"\n"), 0o600); err != nil {
		return "", err
	}

	return secret, nil
}

func readSecretFromFile() (string, error) {
	content, err := os.ReadFile(secretKeyPath)
	if err != nil {
		return "", err
	}

	secret := strings.TrimSpace(string(content))
	if secret == "" {
		return "", os.ErrNotExist
	}

	return secret, nil
}

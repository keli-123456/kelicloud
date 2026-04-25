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
	SecretKeyEnv     = "KOMARI_CLOUD_SECRET_KEY"
	SecretKeyFileEnv = "KOMARI_CLOUD_SECRET_FILE"
	secretKeyPath    = "./data/cloud_secret.key"
)

func IsAvailable() bool {
	_, err := LoadOrCreateSecret()
	return err == nil
}

func LoadOrCreateSecret() (string, error) {
	if secret := strings.TrimSpace(os.Getenv(SecretKeyEnv)); secret != "" {
		return secret, nil
	}

	var firstReadErr error
	for _, path := range secretReadPaths() {
		if secret, err := readSecretFromFile(path); err == nil {
			return secret, nil
		} else if !errors.Is(err, os.ErrNotExist) && firstReadErr == nil {
			firstReadErr = err
		}
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	secret := base64.RawURLEncoding.EncodeToString(buf)
	var firstWriteErr error
	for _, path := range secretWritePaths() {
		if err := writeSecretToFile(path, secret); err == nil {
			return secret, nil
		} else if firstWriteErr == nil {
			firstWriteErr = err
		}
	}

	if firstWriteErr != nil {
		return "", firstWriteErr
	}
	if firstReadErr != nil {
		return "", firstReadErr
	}

	return "", os.ErrPermission
}

func defaultSecretKeyFilePath() string {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	configDir = strings.TrimSpace(configDir)
	if configDir == "" {
		return ""
	}
	return filepath.Join(configDir, "komari", "cloud_secret.key")
}

func secretReadPaths() []string {
	envPath := strings.TrimSpace(os.Getenv(SecretKeyFileEnv))
	if envPath != "" {
		return uniqueSecretPaths(envPath)
	}
	return uniqueSecretPaths(secretKeyPath, defaultSecretKeyFilePath())
}

func secretWritePaths() []string {
	envPath := strings.TrimSpace(os.Getenv(SecretKeyFileEnv))
	legacyPath := filepath.Clean(secretKeyPath)
	defaultPath := defaultSecretKeyFilePath()

	if envPath != "" {
		return uniqueSecretPaths(envPath)
	}
	if _, err := os.Stat(legacyPath); err == nil {
		return uniqueSecretPaths(legacyPath, defaultPath)
	}
	return uniqueSecretPaths(defaultPath, legacyPath)
}

func uniqueSecretPaths(paths ...string) []string {
	seen := make(map[string]struct{}, len(paths))
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		path = filepath.Clean(path)
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		result = append(result, path)
	}
	return result
}

func writeSecretToFile(path, secret string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(secret+"\n"), 0o600)
}

func readSecretFromFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	secret := strings.TrimSpace(string(content))
	if secret == "" {
		return "", os.ErrNotExist
	}

	return secret, nil
}

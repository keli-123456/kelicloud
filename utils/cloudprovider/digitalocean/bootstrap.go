package digitalocean

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh"
)

const randomPasswordAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%^&*_-+=."

func GenerateManagedSSHKeyPair(name string) (*ManagedSSHKeyMaterialView, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "komari-managed"
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return nil, err
	}

	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, err
	}

	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privateDER,
	})

	return &ManagedSSHKeyMaterialView{
		Name:       name,
		PublicKey:  strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublicKey))),
		PrivateKey: strings.TrimSpace(string(privatePEM)),
	}, nil
}

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

func BuildRootPasswordUserData(rootPassword, extraUserData string) (string, error) {
	rootPassword = strings.TrimSpace(rootPassword)
	if rootPassword == "" {
		return "", errors.New("root password is empty")
	}

	extraUserData = strings.TrimSpace(extraUserData)
	if strings.HasPrefix(extraUserData, "#cloud-config") {
		return "", errors.New("root password mode cannot be combined with #cloud-config user_data; use shell commands instead")
	}

	var builder strings.Builder
	builder.WriteString("#!/bin/bash\n")
	builder.WriteString("set -eu\n\n")
	builder.WriteString("printf '%s\\n' ")
	builder.WriteString(shellSingleQuote("root:" + rootPassword))
	builder.WriteString(" | chpasswd\n")
	builder.WriteString("passwd -u root >/dev/null 2>&1 || true\n")
	builder.WriteString("install -d -m 0755 /etc/ssh/sshd_config.d\n")
	builder.WriteString("cat >/etc/ssh/sshd_config.d/99-komari-root-password.conf <<'EOF'\n")
	builder.WriteString("PermitRootLogin yes\n")
	builder.WriteString("PasswordAuthentication yes\n")
	builder.WriteString("ChallengeResponseAuthentication no\n")
	builder.WriteString("KbdInteractiveAuthentication no\n")
	builder.WriteString("EOF\n")
	builder.WriteString("(systemctl restart ssh >/dev/null 2>&1 || systemctl restart sshd >/dev/null 2>&1 || service ssh restart >/dev/null 2>&1 || service sshd restart >/dev/null 2>&1 || true)\n")

	if extraUserData != "" {
		builder.WriteString("\n# User-provided startup commands\n")
		builder.WriteString(extraUserData)
		if !strings.HasSuffix(extraUserData, "\n") {
			builder.WriteString("\n")
		}
	}

	return builder.String(), nil
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func ManagedSSHKeyName(token *TokenRecord) string {
	if token == nil {
		return "komari-managed"
	}

	suffix := strings.TrimSpace(token.ID)
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	if suffix == "" {
		suffix = "default"
	}
	return fmt.Sprintf("komari-%s-managed", suffix)
}

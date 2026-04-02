package aws

import (
	"crypto/rand"
	"errors"
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

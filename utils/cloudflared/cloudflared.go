package cloudflared

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

func downloadCloudflared(downloadURL, filePath string) error {
	log.Println("downloading cloudflared...")
	resp, err := http.Get(downloadURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("download cloudflared failed with status %d", resp.StatusCode)
	}

	if strings.HasSuffix(strings.ToLower(downloadURL), ".tgz") {
		return extractCloudflaredTarGz(resp.Body, filePath)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, resp.Body)
	closeErr := out.Close()
	if closeErr != nil {
		return closeErr
	}
	return makeExecutable(filePath)
}

func extractCloudflaredTarGz(src io.Reader, filePath string) error {
	gzipReader, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("cloudflared archive did not contain a binary")
		}
		if err != nil {
			return err
		}
		if header.FileInfo().IsDir() {
			continue
		}

		base := filepath.Base(header.Name)
		if base != "cloudflared" && base != "cloudflared.exe" {
			continue
		}

		out, err := os.Create(filePath)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, tarReader)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return makeExecutable(filePath)
	}
}

func makeExecutable(filePath string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	return os.Chmod(filePath, 0755)
}

func resolveCloudflaredBinary() (string, string, error) {
	fileName := "cloudflared"

	switch runtime.GOOS {
	case "windows":
		fileName = "cloudflared.exe"
		switch runtime.GOARCH {
		case "amd64":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe", nil
		case "386":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-386.exe", nil
		default:
			return "", "", fmt.Errorf("unsupported architecture: %s", runtime.GOARCH)
		}
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64", nil
		case "386":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-386", nil
		case "arm":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm", nil
		case "arm64":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64", nil
		default:
			return "", "", fmt.Errorf("unsupported architecture: %s", runtime.GOARCH)
		}
	case "darwin":
		switch runtime.GOARCH {
		case "amd64":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz", nil
		case "arm64":
			return fileName, "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-arm64.tgz", nil
		default:
			return "", "", fmt.Errorf("unsupported architecture: %s", runtime.GOARCH)
		}
	default:
		return "", "", fmt.Errorf("unsupported os: %s", runtime.GOOS)
	}
}

var (
	cloudflaredCmd *exec.Cmd
	cloudflaredMu  sync.Mutex
)

func RunCloudflared() error {
	token := strings.TrimSpace(os.Getenv("KOMARI_CLOUDFLARED_TOKEN"))
	if token == "" {
		return fmt.Errorf("KOMARI_CLOUDFLARED_TOKEN is required")
	}

	cloudflaredMu.Lock()
	if cloudflaredCmd != nil && cloudflaredCmd.Process != nil {
		cloudflaredMu.Unlock()
		return nil
	}
	cloudflaredMu.Unlock()

	if err := os.MkdirAll("data", 0755); err != nil {
		return err
	}

	fileName, downloadURL, err := resolveCloudflaredBinary()
	if err != nil {
		return err
	}

	filePath := "data/" + fileName
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := downloadCloudflared(downloadURL, filePath)
		if err != nil {
			return err
		}
	}

	args := []string{"tunnel", "--no-autoupdate", "run", "--token"}
	args = append(args, token)

	cmd := exec.Command(filePath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	cloudflaredMu.Lock()
	cloudflaredCmd = cmd
	cloudflaredMu.Unlock()
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			log.Printf("[cloudflared] %s", scanner.Text())
		}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("[cloudflared] %s", scanner.Text())
		}
	}()
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("cloudflared exited with error: %v", err)
		} else {
			log.Println("cloudflared exited successfully")
		}
		cloudflaredMu.Lock()
		if cloudflaredCmd == cmd {
			cloudflaredCmd = nil
		}
		cloudflaredMu.Unlock()
	}()
	log.Println("cloudflared started")
	return nil
}

func Kill() {
	cloudflaredMu.Lock()
	cmd := cloudflaredCmd
	cloudflaredCmd = nil
	cloudflaredMu.Unlock()

	if cmd != nil && cmd.Process != nil {
		err := cmd.Process.Kill()
		if err != nil {
			log.Printf("failed to kill cloudflared: %v", err)
		} else {
			log.Println("cloudflared killed")
		}
	}
}

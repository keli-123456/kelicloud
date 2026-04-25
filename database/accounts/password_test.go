package accounts

import (
	"strings"
	"testing"
)

func TestPasswordHashUsesArgon2idAndVerifies(t *testing.T) {
	hash, err := hashPasswd("Secret!123")
	if err != nil {
		t.Fatalf("hashPasswd returned error: %v", err)
	}
	if !strings.HasPrefix(hash, passwordHashPrefix) {
		t.Fatalf("expected argon2id hash, got %q", hash)
	}

	valid, needsRehash := verifyPasswd(hash, "Secret!123")
	if !valid || needsRehash {
		t.Fatalf("expected argon2id password to verify without rehash, valid=%v needsRehash=%v", valid, needsRehash)
	}

	valid, _ = verifyPasswd(hash, "wrong")
	if valid {
		t.Fatal("expected wrong password to fail")
	}
}

func TestLegacyPasswordHashStillVerifiesAndRequestsRehash(t *testing.T) {
	hash := legacyHashPasswd("Secret!123")

	valid, needsRehash := verifyPasswd(hash, "Secret!123")
	if !valid || !needsRehash {
		t.Fatalf("expected legacy password to verify and request rehash, valid=%v needsRehash=%v", valid, needsRehash)
	}
}

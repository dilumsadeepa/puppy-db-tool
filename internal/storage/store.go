package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime"

	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/sha3"

	"puppy-db-tool-desktop/internal/model"
)

const (
	storeDirName  = ".puppydb"
	storeFileName = "store.enc"
	saltFileName  = "salt.bin"
	iterations    = 150_000
)

type Store struct {
	dir      string
	filePath string
	saltPath string
}

func NewStore() (*Store, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}

	dir := filepath.Join(home, storeDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}

	return &Store{
		dir:      dir,
		filePath: filepath.Join(dir, storeFileName),
		saltPath: filepath.Join(dir, saltFileName),
	}, nil
}

func (s *Store) Load() (model.AppData, error) {
	var data model.AppData
	blob, err := os.ReadFile(s.filePath)
	if errors.Is(err, os.ErrNotExist) {
		return model.AppData{}, nil
	}
	if err != nil {
		return data, fmt.Errorf("read store file: %w", err)
	}

	plain, err := s.decrypt(blob)
	if err != nil {
		return data, fmt.Errorf("decrypt store: %w", err)
	}

	if err := json.Unmarshal(plain, &data); err != nil {
		return data, fmt.Errorf("decode store: %w", err)
	}

	return data, nil
}

func (s *Store) Save(data model.AppData) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}

	blob, err := s.encrypt(raw)
	if err != nil {
		return fmt.Errorf("encrypt store: %w", err)
	}

	tmp := s.filePath + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o600); err != nil {
		return fmt.Errorf("write temp store: %w", err)
	}
	if err := os.Rename(tmp, s.filePath); err != nil {
		return fmt.Errorf("commit store: %w", err)
	}

	return nil
}

func (s *Store) encrypt(plain []byte) ([]byte, error) {
	key, err := s.key()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	sealed := gcm.Seal(nil, nonce, plain, nil)
	return append(nonce, sealed...), nil
}

func (s *Store) decrypt(blob []byte) ([]byte, error) {
	key, err := s.key()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(blob) <= nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, payload := blob[:nonceSize], blob[nonceSize:]
	return gcm.Open(nil, nonce, payload, nil)
}

func (s *Store) key() ([]byte, error) {
	salt, err := s.loadOrCreateSalt()
	if err != nil {
		return nil, err
	}
	base, err := machineSecret()
	if err != nil {
		return nil, err
	}

	return pbkdf2.Key([]byte(base), salt, iterations, 32, sha3.New256), nil
}

func (s *Store) loadOrCreateSalt() ([]byte, error) {
	blob, err := os.ReadFile(s.saltPath)
	if err == nil {
		if len(blob) < 16 {
			return nil, errors.New("invalid salt")
		}
		return blob, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	salt := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}
	if err := os.WriteFile(s.saltPath, salt, 0o600); err != nil {
		return nil, err
	}
	return salt, nil
}

func machineSecret() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	usr, err := user.Current()
	if err != nil {
		return hostname + "|" + runtime.GOOS + "|fallback", nil
	}
	return usr.Username + "|" + usr.Uid + "|" + hostname + "|" + runtime.GOOS, nil
}

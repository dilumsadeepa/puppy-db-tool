package db

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"puppy-db-tool-desktop/internal/model"
)

type Tunnel struct {
	listener net.Listener
	client   *ssh.Client
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func StartTunnel(cfg model.SSHConfig, remoteHost string, remotePort int) (*Tunnel, int, error) {
	auth, err := sshAuth(cfg)
	if err != nil {
		return nil, 0, err
	}

	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	sshAddr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	client, err := ssh.Dial("tcp", sshAddr, sshConfig)
	if err != nil {
		return nil, 0, fmt.Errorf("ssh dial failed: %w", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		client.Close()
		return nil, 0, fmt.Errorf("local tunnel listen failed: %w", err)
	}

	tunnel := &Tunnel{listener: listener, client: client}
	remoteAddr := net.JoinHostPort(remoteHost, strconv.Itoa(remotePort))
	tunnel.wg.Add(1)
	go tunnel.acceptLoop(remoteAddr)

	localPort := listener.Addr().(*net.TCPAddr).Port
	return tunnel, localPort, nil
}

func (t *Tunnel) acceptLoop(remoteAddr string) {
	defer t.wg.Done()
	for {
		localConn, err := t.listener.Accept()
		if err != nil {
			return
		}
		t.wg.Add(1)
		go t.proxy(localConn, remoteAddr)
	}
}

func (t *Tunnel) proxy(localConn net.Conn, remoteAddr string) {
	defer t.wg.Done()
	defer localConn.Close()

	remoteConn, err := t.client.Dial("tcp", remoteAddr)
	if err != nil {
		return
	}
	defer remoteConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(remoteConn, localConn)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(localConn, remoteConn)
	}()
	wg.Wait()
}

func (t *Tunnel) Close() {
	t.stopOnce.Do(func() {
		if t.listener != nil {
			_ = t.listener.Close()
		}
		if t.client != nil {
			_ = t.client.Close()
		}
		t.wg.Wait()
	})
}

func sshAuth(cfg model.SSHConfig) (ssh.AuthMethod, error) {
	if cfg.AuthType == model.SSHAuthPassword {
		return ssh.Password(cfg.Password), nil
	}

	blob, err := os.ReadFile(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	var signer ssh.Signer
	if cfg.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(blob, []byte(cfg.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(blob)
	}
	if err != nil {
		return nil, fmt.Errorf("parse key: %w", err)
	}
	return ssh.PublicKeys(signer), nil
}

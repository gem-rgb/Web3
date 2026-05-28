package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// TLSFiles identifies the material required to establish mTLS.
type TLSFiles struct {
	CAFile   string
	CertFile string
	KeyFile  string
	ServerName string
}

// ServerTLSConfig returns a server-side TLS configuration that requires client certificates.
func ServerTLSConfig(files TLSFiles) (*tls.Config, error) {
	if files.CertFile == "" || files.KeyFile == "" {
		return nil, errors.New("server certificate and key are required")
	}
	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, err
	}
	caPool, err := loadCertPool(files.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}

// ClientTLSConfig returns a client-side TLS configuration for service-to-service calls.
func ClientTLSConfig(files TLSFiles) (*tls.Config, error) {
	if files.CertFile == "" || files.KeyFile == "" {
		return nil, errors.New("client certificate and key are required")
	}
	cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, err
	}
	caPool, err := loadCertPool(files.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   files.ServerName,
	}, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if caFile == "" {
		return pool, nil
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	if ok := pool.AppendCertsFromPEM(data); !ok {
		return nil, errors.New("failed to parse ca certificate")
	}
	return pool, nil
}


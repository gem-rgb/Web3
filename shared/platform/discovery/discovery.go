package discovery

import "fmt"

// KubernetesServiceDNS returns the canonical in-cluster DNS address.
func KubernetesServiceDNS(service, namespace string, port int) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", service, namespace, port)
}

// HeadlessServiceDNS returns the DNS form used by stateful sets.
func HeadlessServiceDNS(service, namespace string, port int, ordinal int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d", service, ordinal, service, namespace, port)
}

// Localhost returns a loopback endpoint for local development.
func Localhost(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}


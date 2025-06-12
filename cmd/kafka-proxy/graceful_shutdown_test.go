package server

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/grepplabs/kafka-proxy/config"
)

func TestGracefulShutdown(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a temporary config for testing
	testConfig := createTestConfig(t)
	
	// Start kafka-proxy in a separate process
	cmd := startKafkaProxy(t, testConfig)
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	// Wait for the proxy to start
	waitForProxyStart(t, testConfig.Http.ListenAddress)

	// Send SIGTERM to initiate graceful shutdown
	err := cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("Failed to send SIGTERM: %v", err)
	}

	// Wait for the process to exit gracefully
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		// Process should exit without error (exit code 0)
		if err != nil {
			// Check if it's just a signal exit
			if exitError, ok := err.(*exec.ExitError); ok {
				// Exit code 1 is expected when shutting down via signal
				if exitError.ExitCode() != 1 {
					t.Errorf("Unexpected exit code: got %d, want 1", exitError.ExitCode())
				}
			}
		}
	case <-time.After(45 * time.Second):
		t.Fatal("Graceful shutdown took too long")
	}
}

func TestGracefulShutdownWithTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create config with shorter timeout
	testConfig := createTestConfig(t)
	
	// Start kafka-proxy with short shutdown timeout
	cmd := startKafkaProxyWithTimeout(t, testConfig, "5s")
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	// Wait for the proxy to start
	waitForProxyStart(t, testConfig.Http.ListenAddress)

	// Send SIGTERM
	err := cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("Failed to send SIGTERM: %v", err)
	}

	// Measure shutdown time
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
		shutdownDuration := time.Since(start)
		// Should shutdown within reasonable time (much less than default 30s)
		if shutdownDuration >= 15*time.Second {
			t.Errorf("Shutdown took too long: %v, expected < 15s", shutdownDuration)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Shutdown timeout test took too long")
	}
}

func TestHTTPServerGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testConfig := createTestConfig(t)
	
	cmd := startKafkaProxy(t, testConfig)
	defer func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	}()

	// Wait for the proxy to start
	waitForProxyStart(t, testConfig.Http.ListenAddress)

	// Verify HTTP endpoints are working
	resp, err := http.Get(fmt.Sprintf("http://%s%s", testConfig.Http.ListenAddress, testConfig.Http.HealthPath))
	if err != nil {
		t.Fatalf("Failed to get health endpoint: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %d", resp.StatusCode)
	}

	// Start shutdown
	err = cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("Failed to send SIGTERM: %v", err)
	}

	// Wait for shutdown to complete
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
		// After shutdown, HTTP endpoints should not be accessible
		_, err := http.Get(fmt.Sprintf("http://%s%s", testConfig.Http.ListenAddress, testConfig.Http.HealthPath))
		if err == nil {
			t.Error("HTTP endpoint should not be accessible after shutdown")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("HTTP server shutdown test took too long")
	}
}

// Helper functions

func createTestConfig(t *testing.T) *config.Config {
	// Find available ports
	httpPort := findAvailablePort(t)
	proxyPort := findAvailablePort(t)
	
	return &config.Config{
		Http: struct {
			ListenAddress string
			MetricsPath   string
			HealthPath    string
			Disable       bool
		}{
			ListenAddress: fmt.Sprintf("127.0.0.1:%d", httpPort),
			MetricsPath:   "/metrics",
			HealthPath:    "/health",
			Disable:       false,
		},
		Proxy: struct {
			DefaultListenerIP         string
			BootstrapServers          []config.ListenerConfig
			ExternalServers           []config.ListenerConfig
			DeterministicListeners    bool
			DialAddressMappings       []config.DialAddressMapping
			DisableDynamicListeners   bool
			DynamicAdvertisedListener string
			DynamicSequentialMinPort  uint16
			DynamicSequentialMaxPorts uint16
			RequestBufferSize         int
			ResponseBufferSize        int
			ListenerReadBufferSize    int
			ListenerWriteBufferSize   int
			ListenerKeepAlive         time.Duration
			ShutdownTimeout           time.Duration
			TLS                       struct {
				Enable                   bool
				Refresh                  time.Duration
				ListenerCertFile         string
				ListenerKeyFile          string
				ListenerKeyPassword      string
				ListenerCAChainCertFile  string
				ListenerCRLFile          string
				ListenerCipherSuites     []string
				ListenerCurvePreferences []string
				ClientCert               struct {
					Subjects []string
				}
			}
		}{
			DefaultListenerIP: "127.0.0.1",
			BootstrapServers: []config.ListenerConfig{
				{
					BrokerAddress:     "127.0.0.1:9092", // Fake Kafka broker
					ListenerAddress:   fmt.Sprintf("127.0.0.1:%d", proxyPort),
					AdvertisedAddress: fmt.Sprintf("127.0.0.1:%d", proxyPort),
				},
			},
			DisableDynamicListeners: true,
			RequestBufferSize:       4096,
			ResponseBufferSize:      4096,
			ListenerKeepAlive:       60 * time.Second,
			ShutdownTimeout:         30 * time.Second,
		},
	}
}

func startKafkaProxy(t *testing.T, cfg *config.Config) *exec.Cmd {
	return startKafkaProxyWithTimeout(t, cfg, "30s")
}

func startKafkaProxyWithTimeout(t *testing.T, cfg *config.Config, timeout string) *exec.Cmd {
	// Build the command arguments
	args := []string{
		"server",
		"--bootstrap-server-mapping", fmt.Sprintf("%s,%s", 
			cfg.Proxy.BootstrapServers[0].BrokerAddress, 
			cfg.Proxy.BootstrapServers[0].ListenerAddress),
		"--http-listen-address", cfg.Http.ListenAddress,
		"--shutdown-timeout", timeout,
		"--log-level", "info",
		"--dynamic-listeners-disable",
	}

	// Get the path to the kafka-proxy binary
	binaryPath, err := os.Executable()
	if err != nil {
		t.Fatalf("Failed to get executable path: %v", err)
	}
	
	// If we're running tests, the binary might be in a different location
	if strings.Contains(binaryPath, "test") {
		// Try to find the kafka-proxy binary in the current directory or build it
		if _, err := os.Stat("./kafka-proxy"); os.IsNotExist(err) {
			t.Skip("kafka-proxy binary not found, skipping integration test")
		}
		binaryPath = "./kafka-proxy"
	}

	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start kafka-proxy: %v", err)
	}

	return cmd
}

func waitForProxyStart(t *testing.T, address string) {
	// Wait for HTTP server to be ready
	client := &http.Client{Timeout: 1 * time.Second}
	
	for i := 0; i < 30; i++ {
		resp, err := client.Get(fmt.Sprintf("http://%s/health", address))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	
	t.Fatal("Kafka proxy did not start within expected time")
}

func findAvailablePort(t *testing.T) int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	defer listener.Close()
	
	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port
}

// Benchmark graceful shutdown performance
func BenchmarkGracefulShutdown(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	for i := 0; i < b.N; i++ {
		testConfig := createTestConfig(&testing.T{})
		
		cmd := startKafkaProxy(&testing.T{}, testConfig)
		
		// Wait for startup
		waitForProxyStart(&testing.T{}, testConfig.Http.ListenAddress)
		
		// Measure shutdown time
		start := time.Now()
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		shutdownDuration := time.Since(start)
		
		b.ReportMetric(float64(shutdownDuration.Milliseconds()), "shutdown_ms")
	}
}
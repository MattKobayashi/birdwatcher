package bird

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Test_RedisCacheAccess(t *testing.T) {

	cache, err := NewRedisCache(CacheConfig{
		RedisServer: "localhost:6379",
	})

	if err != nil {
		t.Log("Redis server not available:", err)
		t.Log("Skipping redis tests.")
		return
	}

	parsed := Parsed{
		"foo": 23,
		"bar": 42,
		"baz": true,
		"ttl": time.Now().Add(10 * time.Second),
	}

	t.Log("Setting redis cache...")
	err = cache.Set("testkey", parsed, 5)
	if err != nil {
		t.Error(err)
	}

	t.Log("Fetching from redis...")
	parsed, err = cache.Get("testkey")
	if err != nil {
		t.Error(err)
	}

	t.Log(parsed)
}

func Test_RedisCacheAccessKeyMissing(t *testing.T) {

	cache, err := NewRedisCache(CacheConfig{
		RedisServer: "localhost:6379",
	})

	if err != nil {
		t.Log("Redis server not available:", err)
		t.Log("Skipping redis tests.")
		return
	}

	parsed, err := cache.Get("test_missing_key")
	if err == nil {
		t.Error(err)
	}
	t.Log("Cache error:", err)
	t.Log(parsed)
}

func Test_RedisCacheRoutes(t *testing.T) {
	f, err := openFile("routes_bird1_ipv4.sample")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	parsed := parseRoutes(f)
	_, ok := parsed["routes"].([]Parsed)
	if !ok {
		t.Fatal("Error getting routes")
	}

	cache, err := NewRedisCache(CacheConfig{
		RedisServer: "localhost:6379",
	})

	if err != nil {
		t.Log("Redis server not available:", err)
		t.Log("Skipping redis tests.")
		return
	}

	err = cache.Set("routes_protocol_test", parsed, 5)
	if err != nil {
		t.Error(err)
	}

	parsed, err = cache.Get("routes_protocol_test")
	if err != nil {
		t.Error(err)
		return
	}

	routes, ok := parsed["routes"].([]interface{})
	if !ok {
		t.Error("Error getting routes")
	}

	t.Log("Retrieved routes:", len(routes))
}

func startFakeRedisServer(t *testing.T, payload string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if !strings.HasPrefix(line, "*") {
						continue
					}
					n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
					if err != nil || n <= 0 {
						return
					}
					args := make([]string, 0, n)
					for i := 0; i < n; i++ {
						hdr, err := r.ReadString('\n')
						if err != nil || !strings.HasPrefix(hdr, "$") {
							return
						}
						size, err := strconv.Atoi(strings.TrimSpace(hdr[1:]))
						if err != nil || size < 0 {
							return
						}
						buf := make([]byte, size+2)
						if _, err := io.ReadFull(r, buf); err != nil {
							return
						}
						args = append(args, string(buf[:size]))
					}
					switch strings.ToUpper(args[0]) {
					case "PING":
						fmt.Fprint(c, "+PONG\r\n")
					case "GET":
						fmt.Fprintf(c, "$%d\r\n%s\r\n", len(payload), payload)
					default:
						fmt.Fprint(c, "+OK\r\n")
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), func() {
		ln.Close()
		<-done
	}
}

func TestRedisCacheExpiredInbandTTL(t *testing.T) {
	expired := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	addr, closeServer := startFakeRedisServer(t, fmt.Sprintf(`{"ttl":%q,"value":1}`, expired))
	defer closeServer()

	c, err := NewRedisCache(CacheConfig{RedisServer: addr})
	if err != nil {
		t.Fatalf("Failed to connect to fake Redis server: %v", err)
	}
	defer c.client.Close()

	parsed, err := c.Get("expired-key")
	if err == nil {
		t.Fatalf("Expected error for expired in-band TTL, got nil (parsed: %v)", parsed)
	}
	if parsed != nil {
		t.Errorf("Expected NilParse on expired in-band TTL, got %v", parsed)
	}
}

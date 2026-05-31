// Package ads wraps the MaxMind GeoIP2 database reader for IP-based country look-ups
// and anonymous-IP detection (VPN, datacenter, Tor, public proxy). Reader access is
// protected by a RWMutex to allow atomic hot-reload without closing active look-ups:
// the reader pointer is swapped under write lock; ongoing look-ups hold the read lock
// across the Lookup call, preventing use-after-close.
//
// Lookup target structs (countryResult, anonymousIPResult) are pooled to avoid a
// per-call heap allocation; fields are zeroed explicitly before each use because
// maxminddb.Lookup does not zero the target before unmarshalling.
package ads

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/oschwald/maxminddb-golang"
)

// GeoProvider abstracts IP geo-lookup and anonymous-IP detection. The interface
// is satisfied by MaxMindProvider in production and by MockGeoProvider in tests.
// Close must be called when the provider is no longer needed to release file handles.
type GeoProvider interface {
	GetCountry(ip string) (string, error)
	IsAnonymous(ip string) (bool, error)
	Close() error
}

type countryResult struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
}

type anonymousIPResult struct {
	IsAnonymous       bool `maxminddb:"is_anonymous"`
	IsAnonymousVPN    bool `maxminddb:"is_anonymous_vpn"`
	IsHostingProvider bool `maxminddb:"is_hosting_provider"`
	IsPublicProxy     bool `maxminddb:"is_public_proxy"`
	IsTorExitNode     bool `maxminddb:"is_tor_exit_node"`
}

var countryPool = sync.Pool{
	New: func() any {
		return &countryResult{}
	},
}

var anonymousIPPool = sync.Pool{
	New: func() any {
		return &anonymousIPResult{}
	},
}

// MaxMindProvider wraps a maxminddb.Reader with a RWMutex to allow atomic swap
// of the underlying database file without service interruption. The zero value
// is invalid; use NewMaxMindProvider.
type MaxMindProvider struct {
	reader *maxminddb.Reader
	mu     sync.RWMutex
}

func NewMaxMindProvider(dbPath string) (*MaxMindProvider, error) {
	db, err := maxminddb.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open maxmind db: %w", err)
	}
	return &MaxMindProvider{reader: db}, nil
}

// GetCountry returns the ISO 3166-1 alpha-2 country code for ipStr, or an error
// if the IP is malformed or the provider has been closed. An empty string is
// returned for IPs not present in the database (private ranges, etc.).
func (p *MaxMindProvider) GetCountry(ipStr string) (string, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return "", fmt.Errorf("invalid IP: %s", ipStr)
	}

	p.mu.RLock()
	reader := p.reader
	p.mu.RUnlock()
	if reader == nil {
		return "", fmt.Errorf("geoip provider closed")
	}

	record := countryPool.Get().(*countryResult)
	record.Country.IsoCode = ""
	defer countryPool.Put(record)

	if err := reader.Lookup(ip, record); err != nil {
		return "", err
	}

	return record.Country.IsoCode, nil
}

// IsAnonymous returns true if the IP is classified as an anonymous VPN, hosting
// provider, public proxy, or Tor exit node. All five MaxMind anonymous-IP fields
// are OR-combined; a false positive rate exists for shared hosting IPs.
func (p *MaxMindProvider) IsAnonymous(ipStr string) (bool, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, fmt.Errorf("invalid IP: %s", ipStr)
	}

	p.mu.RLock()
	reader := p.reader
	p.mu.RUnlock()
	if reader == nil {
		return false, fmt.Errorf("geoip provider closed")
	}

	record := anonymousIPPool.Get().(*anonymousIPResult)
	record.IsAnonymous = false
	record.IsAnonymousVPN = false
	record.IsHostingProvider = false
	record.IsPublicProxy = false
	record.IsTorExitNode = false
	defer anonymousIPPool.Put(record)

	if err := reader.Lookup(ip, record); err != nil {
		return false, err
	}

	return record.IsAnonymous || record.IsAnonymousVPN || record.IsHostingProvider || record.IsPublicProxy || record.IsTorExitNode, nil
}

func (p *MaxMindProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reader != nil {
		err := p.reader.Close()
		p.reader = nil
		return err
	}
	return nil
}

// MockGeoProvider is a test stub that returns pre-seeded country codes and flags
// IPs ending in .66 or .77 as anonymous. It satisfies the GeoProvider interface
// without requiring a MaxMind database file.
type MockGeoProvider struct {
	Countries map[string]string
}

func (p *MockGeoProvider) GetCountry(ip string) (string, error) {
	if code, ok := p.Countries[ip]; ok {
		return code, nil
	}
	return "US", nil
}

func (p *MockGeoProvider) IsAnonymous(ip string) (bool, error) {
	return strings.HasSuffix(ip, ".66") || strings.HasSuffix(ip, ".77"), nil
}

func (p *MockGeoProvider) Close() error { return nil }

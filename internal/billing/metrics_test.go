package billing

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"espx/pkg/lifecycle"

	"github.com/stretchr/testify/require"
)

func TestBillingMetrics_exposedOnHTTP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	metricsSrv := lifecycle.StartMetrics(fmt.Sprintf("127.0.0.1:%d", port))
	defer func() {
		require.NoError(t, metricsSrv.Shutdown(2*time.Second))
	}()

	InvoicesGeneratedTotal.Inc()
	InvoiceErrorsTotal.WithLabelValues("test").Inc()
	LedgerDriftTotal.Inc()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		text := string(body)
		for _, want := range []string{
			"billing_invoices_generated_total",
			"billing_invoice_errors_total",
			"billing_ledger_drift_total",
		} {
			if !strings.Contains(text, want) {
				return false
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

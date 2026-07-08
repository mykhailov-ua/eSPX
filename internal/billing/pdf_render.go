package billing

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"espx/internal/billing/pb"
)

// RenderInvoicePDF produces a minimal PDF 1.4 document for invoice delivery.
func RenderInvoicePDF(inv *pb.Invoice) []byte {
	if inv == nil {
		return nil
	}

	month := ""
	if inv.BillingMonth != nil {
		month = inv.BillingMonth.AsTime().UTC().Format("2006-01")
	}

	var lines strings.Builder
	lines.WriteString(fmt.Sprintf("Invoice %s\n", inv.Id))
	lines.WriteString(fmt.Sprintf("Customer %s\n", inv.CustomerId))
	lines.WriteString(fmt.Sprintf("Period %s\n", month))
	lines.WriteString(fmt.Sprintf("Currency %s\n", inv.Currency))
	lines.WriteString(fmt.Sprintf("Subtotal %s\n", formatPDFMicro(inv.SubtotalMicro)))
	lines.WriteString(fmt.Sprintf("Tax %s\n", formatPDFMicro(inv.TaxMicro)))
	lines.WriteString(fmt.Sprintf("Total %s\n", formatPDFMicro(inv.TotalMicro)))
	for _, line := range inv.Lines {
		lines.WriteString(fmt.Sprintf("  %s: %s (%d entries)\n",
			line.LedgerType,
			formatPDFMicro(line.AmountMicro),
			line.EntryCount,
		))
	}
	lines.WriteString(fmt.Sprintf("Generated %s UTC\n", time.Now().UTC().Format(time.RFC3339)))

	content := lines.String()
	stream := escapePDFText(content)

	var buf bytes.Buffer
	w := func(s string) { _, _ = buf.WriteString(s) }

	w("%PDF-1.4\n")
	offsets := []int{0}

	writeObj := func(n int, body string) {
		offsets = append(offsets, buf.Len())
		w(strconv.Itoa(n))
		w(" 0 obj\n")
		w(body)
		w("\nendobj\n")
	}

	writeObj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	writeObj(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
	writeObj(3, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>")
	writeObj(4, fmt.Sprintf("<< /Length %d >>\nstream\nBT /F1 10 Tf 50 740 Td (%s) Tj ET\nendstream", len(stream)+32, stream))
	writeObj(5, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

	xrefPos := buf.Len()
	w("xref\n")
	w(fmt.Sprintf("0 %d\n", len(offsets)))
	w("0000000000 65535 f \n")
	for i := 1; i < len(offsets); i++ {
		w(fmt.Sprintf("%010d 00000 n \n", offsets[i]))
	}
	w("trailer\n")
	w(fmt.Sprintf("<< /Size %d /Root 1 0 R >>\n", len(offsets)))
	w("startxref\n")
	w(strconv.Itoa(xrefPos))
	w("\n%%EOF\n")

	return buf.Bytes()
}

func formatPDFMicro(micro int64) string {
	whole := micro / 1_000_000
	frac := micro % 1_000_000
	if frac < 0 {
		frac = -frac
	}
	return fmt.Sprintf("%d.%06d", whole, frac)
}

func escapePDFText(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "(", "\\(")
	s = strings.ReplaceAll(s, ")", "\\)")
	s = strings.ReplaceAll(s, "\n", ") Tj T* (")
	return s
}

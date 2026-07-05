package payment

import "espx/internal/payment/db"

// FinancialFindingSeverity classifies recon findings for ops alerting thresholds.
type FinancialFindingSeverity int

const (
	SeverityInfo FinancialFindingSeverity = iota
	SeverityWarn
	SeverityCritical
)

func financialFindingSeverity(kind db.PaymentFinancialFindingKind) FinancialFindingSeverity {
	switch kind {
	case db.PaymentFinancialFindingKindMISSINGLEDGERTOPUP,
		db.PaymentFinancialFindingKindTOPUPAMOUNTMISMATCH,
		db.PaymentFinancialFindingKindSETTLEMENTFAILEDINTENT:
		return SeverityCritical
	case db.PaymentFinancialFindingKindORPHANLEDGERTOPUP,
		db.PaymentFinancialFindingKindREFUNDLEDGERDRIFT,
		db.PaymentFinancialFindingKindCHARGEBACKLEDGERDRIFT,
		db.PaymentFinancialFindingKindCHARGEBACKREVERSALDRIFT,
		db.PaymentFinancialFindingKindDEADOUTBOX:
		return SeverityWarn
	default:
		return SeverityInfo
	}
}

func severityAtLeastWarn(s FinancialFindingSeverity) bool {
	return s >= SeverityWarn
}

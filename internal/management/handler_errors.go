package management

import (
	"errors"
	"log/slog"
	"net/http"

	"espx/internal/ads"
	"espx/pkg/httpresponse"

	"github.com/jackc/pgx/v5"
)

type validationError string

func (e validationError) Error() string { return string(e) }

func errValidation(msg string) error { return validationError(msg) }

// mapServiceError maps domain failures to stable client-facing codes without leaking store internals.
func mapServiceError(err error) (status int, code, message string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, errForbidden) {
		return http.StatusForbidden, "FORBIDDEN", "forbidden"
	}

	if errors.Is(err, ErrSelfServeActiveCampaignLimit) || errors.Is(err, ErrSelfServeDailyCreateLimit) {
		return http.StatusTooManyRequests, "LIMIT_EXCEEDED", err.Error()
	}

	var q invalidQueryError
	if errors.As(err, &q) {
		return http.StatusBadRequest, "BAD_REQUEST", string(q)
	}

	var ve validationError
	if errors.As(err, &ve) {
		return http.StatusBadRequest, "BAD_REQUEST", string(ve)
	}

	if isNotFoundError(err) {
		return http.StatusNotFound, "NOT_FOUND", "resource not found"
	}

	if isConflictError(err) {
		return http.StatusConflict, "CONFLICT", conflictMessage(err)
	}

	if errors.Is(err, ErrSellersJSONInvalid) {
		return http.StatusServiceUnavailable, "SUPPLY_INVALID", ErrSellersJSONInvalid.Error()
	}

	if msg, ok := badRequestMessage(err); ok {
		return http.StatusBadRequest, "BAD_REQUEST", msg
	}

	return http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"
}

func isNotFoundError(err error) bool {
	return errors.Is(err, pgx.ErrNoRows) ||
		errors.Is(err, ErrCustomerNotFound) ||
		errors.Is(err, ErrPaymentTopupNotFound) ||
		errors.Is(err, ErrCampaignNotFound) ||
		errors.Is(err, ErrBrandNotFound) ||
		errors.Is(err, ErrCreativeNotFound) ||
		errors.Is(err, ErrTemplateNotFound) ||
		errors.Is(err, ErrRtbDealNotFound) ||
		errors.Is(err, ErrDealCustomerMissing) ||
		errors.Is(err, ErrSellerNotFound) ||
		errors.Is(err, ErrAdsTxtEntryNotFound) ||
		errors.Is(err, ads.ErrSlotMapVersionNotFound)
}

func isConflictError(err error) bool {
	return errors.Is(err, ErrSlotMigrationNotReady) || errors.Is(err, ads.ErrSlotMapAlreadyActive)
}

func conflictMessage(err error) string {
	switch {
	case errors.Is(err, ErrSlotMigrationNotReady):
		return ErrSlotMigrationNotReady.Error()
	case errors.Is(err, ads.ErrSlotMapAlreadyActive):
		return ads.ErrSlotMapAlreadyActive.Error()
	default:
		return "conflict"
	}
}

func badRequestMessage(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrInsufficientBalance):
		return ErrInsufficientBalance.Error(), true
	case errors.Is(err, ErrSelfServeActiveCampaignLimit),
		errors.Is(err, ErrSelfServeDailyCreateLimit),
		errors.Is(err, ErrSelfServeBudgetOutOfRange):
		return err.Error(), true
	case errors.Is(err, ErrBrandBelongsToAnotherCustomer),
		errors.Is(err, ErrTemplateBelongsToAnotherCustomer),
		errors.Is(err, ErrCampaignCannotBePaused),
		errors.Is(err, ErrCampaignNotPaused),
		errors.Is(err, ErrCampaignOutsideSchedule),
		errors.Is(err, ErrInvalidPacingMode),
		errors.Is(err, ErrWeightMustBePositive),
		errors.Is(err, ErrCreativeStatusInvalid),
		errors.Is(err, ErrIncompleteIdempotency),
		errors.Is(err, ErrUnsupportedGranularity),
		errors.Is(err, ErrInvalidTimeRange),
		errors.Is(err, ErrInvalidServiceFilter),
		errors.Is(err, ErrInvalidDealPacing),
		errors.Is(err, ErrDuplicateDealID),
		errors.Is(err, ErrInvalidDealSeats),
		errors.Is(err, ErrInvalidSellerType),
		errors.Is(err, ErrInvalidRelationship),
		errors.Is(err, ErrSupplyChainTooLong),
		errors.Is(err, ErrRefundExceedsTopup),
		errors.Is(err, ErrChargebackExceedsTopup),
		errors.Is(err, ErrChargebackReversalExceedsWithdrawn),
		errors.Is(err, errExportLimit),
		errors.Is(err, ads.ErrSlotMapIncomplete),
		errors.Is(err, ads.ErrSlotMapInvalidSlot),
		errors.Is(err, ads.ErrSlotMapInvalidShard):
		return err.Error(), true
	default:
		return "", false
	}
}

// writeServiceError logs server failures and returns a sanitized HTTP error body.
func writeServiceError(w http.ResponseWriter, err error, logAttrs ...any) {
	status, code, message := mapServiceError(err)
	if status >= http.StatusInternalServerError {
		attrs := append([]any{slog.String("error", err.Error())}, logAttrs...)
		slog.Error("management request failed", attrs...)
	}
	httpresponse.Error(w, status, code, message)
}

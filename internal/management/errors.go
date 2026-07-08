package management

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	ErrCustomerNotFound                   = errors.New("customer not found")
	ErrPaymentTopupNotFound               = errors.New("payment topup not found")
	ErrRefundExceedsTopup                 = errors.New("refund exceeds settled topup")
	ErrChargebackExceedsTopup             = errors.New("chargeback exceeds settled topup")
	ErrChargebackReversalExceedsWithdrawn = errors.New("chargeback reversal exceeds withdrawn amount")

	ErrCampaignNotFound = errors.New("campaign not found")
	ErrBrandNotFound    = errors.New("brand not found")
	ErrCreativeNotFound = errors.New("creative not found")
	ErrTemplateNotFound = errors.New("template not found")

	ErrInsufficientBalance              = errors.New("insufficient balance")
	ErrBrandBelongsToAnotherCustomer    = errors.New("brand belongs to another customer")
	ErrTemplateBelongsToAnotherCustomer = errors.New("template belongs to another customer")
	ErrCampaignCannotBePaused           = errors.New("campaign cannot be paused")
	ErrCampaignNotPaused                = errors.New("campaign is not paused")
	ErrCampaignOutsideSchedule          = errors.New("campaign is outside scheduled delivery window")
	ErrInvalidPacingMode                = errors.New("invalid pacing mode")
	ErrWeightMustBePositive             = errors.New("weight must be positive")
	ErrCreativeStatusInvalid            = errors.New("status must be ACTIVE or PAUSED")
	ErrIncompleteIdempotency            = errors.New("incomplete idempotency")
	ErrUnsupportedGranularity           = errors.New("unsupported granularity")
	ErrInvalidTimeRange                 = errors.New("invalid time range")
	ErrInvalidServiceFilter             = errors.New("invalid service filter")

	ErrSelfServeActiveCampaignLimit = errors.New("self-serve active campaign limit reached")
	ErrSelfServeDailyCreateLimit    = errors.New("self-serve daily campaign create limit reached")
	ErrSelfServeBudgetOutOfRange    = errors.New("self-serve budget out of allowed range")
)

// mapNotFound maps pgx.ErrNoRows to a domain not-found sentinel; other errors pass through.
func mapNotFound(err error, notFound error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound
	}
	return err
}

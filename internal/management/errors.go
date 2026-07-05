package management

import "errors"

var ErrCustomerNotFound = errors.New("customer not found")
var ErrPaymentTopupNotFound = errors.New("payment topup not found")
var ErrRefundExceedsTopup = errors.New("refund exceeds settled topup")
var ErrChargebackExceedsTopup = errors.New("chargeback exceeds settled topup")
var ErrChargebackReversalExceedsWithdrawn = errors.New("chargeback reversal exceeds withdrawn amount")

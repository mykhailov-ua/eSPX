package management

import "errors"

// ErrCustomerNotFound signals a missing billing account during settlement or balance updates.
var ErrCustomerNotFound = errors.New("customer not found")

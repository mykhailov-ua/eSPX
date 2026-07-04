package ads

import "errors"

// ErrBrokerPayloadUnrecognized means broker bytes are neither stream nor audit log records.
var ErrBrokerPayloadUnrecognized = errors.New("unrecognized broker payload format")

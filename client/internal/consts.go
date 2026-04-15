package internal

import "time"

const (
	headerContentType      = "Content-Type"
	contentTypeProtobuf    = "application/x-protobuf"
	headerOpAMPInstanceUID = "OpAMP-Instance-UID"

	// DefaultUnavailableRetryInterval is the spec-recommended minimum retry interval
	// when the server sends UNAVAILABLE without retry_info.
	// See https://github.com/open-telemetry/opamp-spec/blob/main/specification.md#throttling
	DefaultUnavailableRetryInterval = 30 * time.Second
)

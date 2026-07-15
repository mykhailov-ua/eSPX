//go:build !fraudscoring_onnx

package fraudscoring

import (
	"context"
	"errors"
)

// ONNXScorer is a stub when fraudscoring_onnx build tag is not set.
type ONNXScorer struct{}

// NewONNXScorer returns a stub error.
func NewONNXScorer(modelPath string) (*ONNXScorer, error) {
	return nil, errors.New("ONNX scorer is not enabled in this build. Rebuild with -tags fraudscoring_onnx")
}

// Name returns the stub name.
func (o *ONNXScorer) Name() string {
	return "onnx_stub"
}

// Dims returns 0.
func (o *ONNXScorer) Dims() int {
	return 0
}

// ScoreBatch returns a stub error.
func (o *ONNXScorer) ScoreBatch(ctx context.Context, rows []FeatureRow) ([]float64, error) {
	return nil, errors.New("ONNX scorer is not enabled in this build")
}

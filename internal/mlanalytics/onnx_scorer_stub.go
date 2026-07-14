//go:build !ml_onnx

package mlanalytics

import (
	"context"
	"errors"
)

// ONNXScorer is a stub implementation of Scorer when ml_onnx build tag is not active.
type ONNXScorer struct{}

// NewONNXScorer returns a stub error.
func NewONNXScorer(modelPath string) (*ONNXScorer, error) {
	return nil, errors.New("ONNX scorer is not enabled in this build. Rebuild with -tags ml_onnx to enable ONNX support")
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

//go:build ml_onnx

package mlanalytics

import (
	"context"
	"fmt"

	ort "github.com/yalue/onnxruntime_go"
)

// ONNXScorer implements the Scorer interface using onnxruntime.
type ONNXScorer struct {
	session *ort.AdvancedSession
	dims    int
}

// NewONNXScorer loads an ONNX model and initializes onnxruntime.
func NewONNXScorer(modelPath string) (*ONNXScorer, error) {
	if !ort.IsInitialized() {
		err := ort.Initialize()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize onnxruntime: %w", err)
		}
	}

	// Create advanced session
	session, err := ort.NewAdvancedSession(modelPath,
		[]string{"input"},
		[]string{"label", "probabilities"},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create ONNX session: %w", err)
	}

	return &ONNXScorer{
		session: session,
		dims:    7,
	}, nil
}

// Name returns the scorer name.
func (o *ONNXScorer) Name() string {
	return "onnx_iforest"
}

// Dims returns the feature dimensions.
func (o *ONNXScorer) Dims() int {
	return o.dims
}

// ScoreBatch scores a batch of FeatureRow.
func (o *ONNXScorer) ScoreBatch(ctx context.Context, rows []FeatureRow) ([]float64, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	nRows := len(rows)
	nCols := o.dims

	flat := make([]float32, nRows*nCols)
	for i, row := range rows {
		vec := row.ToVector()
		for j := 0; j < nCols; j++ {
			if j < len(vec) {
				flat[i*nCols+j] = float32(vec[j])
			} else {
				flat[i*nCols+j] = 0.0
			}
		}
	}

	inputShape := ort.NewShape(int64(nRows), int64(nCols))
	inputTensor, err := ort.NewTensor(inputShape, flat)
	if err != nil {
		return nil, fmt.Errorf("failed to create input tensor: %w", err)
	}
	defer inputTensor.Destroy()

	outputTensor, err := ort.NewEmptyTensor[float32](ort.NewShape(int64(nRows)))
	if err != nil {
		return nil, fmt.Errorf("failed to create output tensor: %w", err)
	}
	defer outputTensor.Destroy()

	err = o.session.Run(
		[]ort.ArbitraryTensor{inputTensor},
		[]ort.ArbitraryTensor{outputTensor},
	)
	if err != nil {
		return nil, fmt.Errorf("ONNX run failed: %w", err)
	}

	predictions := outputTensor.GetData()
	out := make([]float64, nRows)
	for i, pred := range predictions {
		out[i] = float64(pred)
	}

	return out, nil
}

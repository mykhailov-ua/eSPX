package mlanalytics

import (
	"context"
	"log/slog"
	"sync"

	"github.com/zhongdai/go-lgbm"
)

// Scorer defines the interface for batch scoring.
type Scorer interface {
	Name() string
	ScoreBatch(ctx context.Context, rows []FeatureRow) ([]float64, error)
	Dims() int
}

// LGBMScorer implements the Scorer interface using LightGBM.
type LGBMScorer struct {
	model *lgbm.Model
	dims  int
	pool  sync.Pool
}

// NewLGBMScorer loads a LightGBM model from a file.
func NewLGBMScorer(modelPath string) (*LGBMScorer, error) {
	model, err := lgbm.ModelFromFile(modelPath, true)
	if err != nil {
		// Try loading with isV4 = false if true fails
		model, err = lgbm.ModelFromFile(modelPath, false)
		if err != nil {
			slog.Error("failed to load LightGBM model", "model_path", modelPath, "error", err)
			return nil, err
		}
	}

	dims := model.NFeatures()
	return &LGBMScorer{
		model: model,
		dims:  dims,
		pool: sync.Pool{
			New: func() any {
				buf := make([]float64, 0, 10000)
				return &buf
			},
		},
	}, nil
}

// Name returns the scorer name.
func (lgbmScorer *LGBMScorer) Name() string {
	return "lightgbm"
}

// Dims returns the number of features expected by the model.
func (lgbmScorer *LGBMScorer) Dims() int {
	return lgbmScorer.dims
}

// ScoreBatch scores a batch of FeatureRow.
func (lgbmScorer *LGBMScorer) ScoreBatch(ctx context.Context, rows []FeatureRow) ([]float64, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	nRows := len(rows)
	nCols := lgbmScorer.dims

	// Get a flat buffer from the pool
	pBuf := lgbmScorer.pool.Get().(*[]float64)
	defer func() {
		*pBuf = (*pBuf)[:0]
		lgbmScorer.pool.Put(pBuf)
	}()

	// Ensure the buffer has enough capacity
	neededCap := nRows * nCols
	if cap(*pBuf) < neededCap {
		*pBuf = make([]float64, neededCap)
	} else {
		*pBuf = (*pBuf)[:neededCap]
	}

	flat := *pBuf

	// Flatten the features into the flat row-major matrix
	for i, row := range rows {
		vec := row.ToVector()
		for j := 0; j < nCols; j++ {
			if j < len(vec) {
				flat[i*nCols+j] = vec[j]
			} else {
				flat[i*nCols+j] = 0.0
			}
		}
	}

	out := make([]float64, nRows)
	// PredictDense(features, nRows, nCols, nEstimators, nThreads, output)
	err := lgbmScorer.model.PredictDense(flat, nRows, nCols, 0, 0, out)
	if err != nil {
		slog.Error("lgbm PredictDense failed", "error", err)
		return nil, err
	}

	return out, nil
}

package main

import (
	"testing"
)

func TestHllSketchAccuracyRunner(t *testing.T) {
	runner := NewDistinctCountAccuracyProfile(distinctCountJobConfig)
	runner.run()
}

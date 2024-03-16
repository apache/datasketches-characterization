package main

import "github.com/apache/datasketches-go/hll"

type distinctCountJobConfigType struct {
	lgMinU int // The starting # of uniques that is printed at the end.
	lgMaxU int // How high the # uniques go
	UPPO   int // The horizontal x-resolution of trials points

	lgMinT int // prints intermediate results starting w/ this lgMinT
	lgMaxT int // The max trials
	TPPO   int // how often intermediate results are printed

	lgQK      int  // size of quantiles sketch
	interData bool // intermediate data

	runner DistinctCountAccuracyProfileRunner
}

var (
	distinctCountJobConfig = distinctCountJobConfigType{
		lgMinU: 0,
		lgMaxU: 20,
		UPPO:   16,

		lgMinT: 8,
		lgMaxT: 20,
		TPPO:   1,

		lgQK:      12,
		interData: true,

		runner: NewHllSketchAccuracyRunner(4 /* lgK */, hll.TgtHllTypeHll8 /* tgtType */),
	}
)

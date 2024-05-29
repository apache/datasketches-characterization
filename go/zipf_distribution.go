/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"math"
	"math/rand"
)

const (
	zipf_taylor_threshold = 1e-8
	zipf_f_1_2            = 0.5
	zipf_f_1_3            = 1.0 / 3
	zipf_f_1_4            = 0.25
)

type zipfDistribution struct {
	numberOfElements int64
	exponent         float64

	hIntegralX1               float64
	hIntegralNumberOfElements float64
	s                         float64
}

func newZipfDistribution(numberOfElements int64, exponent float64) *zipfDistribution {
	d := &zipfDistribution{
		numberOfElements:          numberOfElements,
		exponent:                  exponent,
		hIntegralX1:               hIntegral(1.5, exponent) - 1.0,
		hIntegralNumberOfElements: hIntegral(float64(numberOfElements)+zipf_f_1_2, exponent),
		s:                         2 - hIntegralInverse(hIntegral(2.5, exponent)-h(2.0, exponent), exponent),
	}
	return d
}

func (z *zipfDistribution) sample() int64 {
	for true {
		u := z.hIntegralNumberOfElements + rand.Float64()*(z.hIntegralX1-z.hIntegralNumberOfElements)
		x := hIntegralInverse(u, z.exponent)
		k := int64(x + zipf_f_1_2)
		if k < 1 {
			k = 1
		} else if k > z.numberOfElements {
			k = z.numberOfElements
		}

		if ((float64(k) - x) <= z.s) || (u >= (hIntegral(float64(k)+zipf_f_1_2, z.exponent) - h(float64(k), z.exponent))) {
			return k
		}
	}
	panic("this cannot happen")
}

func h(x float64, exponent float64) float64 {
	return math.Exp(-exponent * math.Log(x))
}

func hIntegral(x float64, exponent float64) float64 {
	return helper2((1-exponent)*math.Log(x)) * math.Log(x)
}

func helper1(x float64) float64 {
	if math.Abs(x) > zipf_taylor_threshold {
		return math.Log1p(x) / x
	}
	return 1 - (x * (zipf_f_1_2 - (x * (zipf_f_1_3 - (zipf_f_1_4 * x)))))
}

func helper2(x float64) float64 {
	if math.Abs(x) > zipf_taylor_threshold {
		return math.Expm1(x) / x
	}
	return 1 + (x * zipf_f_1_2 * (1 + (x * zipf_f_1_3 * (1 + (zipf_f_1_4 * x)))))
}

func hIntegralInverse(x float64, exponent float64) float64 {
	t := x * (1 - exponent)
	if t < -1 {
		// Limit value to the reange [-1, +inf).
		// t could be smaller than -1 in some rare cases due to numerical errors.
		t = -1
	}
	return math.Exp(helper1(t) * x)
}

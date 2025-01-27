# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

clf;

#tdigest_tdunning=load('../../results/tdigest_timing-100.tsv');
td100_dbl=load('../results/tdigest_timing-100-double-gcc13.tsv');
td200_dbl=load('../results/tdigest_timing-200-double-gcc13.tsv');
req10_dbl=load('../results/req_sketch_timing_double_hra_k10_gcc13.tsv');
req20_dbl=load('../results/req_sketch_timing_double_hra_k20_gcc13.tsv');
req30_dbl=load('../results/req_sketch_timing_double_hra_k30_gcc13.tsv');
req40_dbl=load('../results/req_sketch_timing_double_hra_k40_gcc13.tsv');

hold on;

#semilogx(tdigest_tdunning(:,1), tdigest_tdunning(:,11), 'linewidth', 2);

semilogx(td100_dbl(:,1), td100_dbl(:,5), 'linewidth', 2);
semilogx(td200_dbl(:,1), td200_dbl(:,5), 'linewidth', 2);

semilogx(req10_dbl(:,1), req10_dbl(:,5), 'linewidth', 2);
semilogx(req20_dbl(:,1), req20_dbl(:,5), 'linewidth', 2);
semilogx(req30_dbl(:,1), req30_dbl(:,5), 'linewidth', 2);
semilogx(req40_dbl(:,1), req40_dbl(:,5), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Serialized size of TDigest and REQ sketch'
xlabel 'stream size'
ylabel 'serialized size, bytes'
grid minor on
legend(
'tdigest<double> k=100',
'tdigest<double> k=200',
'req\_sketch<double> k=10',
'req\_sketch<double> k=20',
'req\_sketch<double> k=30',
'req\_sketch<double> k=40',
'location', 'northwest'
);

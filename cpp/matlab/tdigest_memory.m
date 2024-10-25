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
#td100_dbl=load('../results/tdigest_double_memory-100.tsv');
#td100_dbl_nres=load('../results/tdigest_double_memory-100-no-reserve.tsv');

td100_flt=load('../results/tdigest_float_memory-100.tsv');
td200_flt=load('../results/tdigest_float_memory-200.tsv');

req10_flt=load('../results/req_sketch_memory_float_k10.tsv');
req20_flt=load('../results/req_sketch_memory_float_k20.tsv');
req30_flt=load('../results/req_sketch_memory_float_k30.tsv');
req40_flt=load('../results/req_sketch_memory_float_k40.tsv');

hold on;

semilogx(td100_flt(:,1), td100_flt(:,11), 'linewidth', 2);
semilogx(td200_flt(:,1), td200_flt(:,11), 'linewidth', 2);

semilogx(req10_flt(:,1), req10_flt(:,11), 'linewidth', 2);
semilogx(req20_flt(:,1), req20_flt(:,11), 'linewidth', 2);
semilogx(req30_flt(:,1), req30_flt(:,11), 'linewidth', 2);
semilogx(req40_flt(:,1), req40_flt(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage TDigest vs REQ sketch (updates, no transients, C++)'
xlabel 'number of values'
ylabel 'size in memory, bytes'
grid minor on
legend(
'tdigest<float> k=100',
'tdigest<float> k=200',
'req\_sketch<float> k=10',
'req\_sketch<float> k=20',
'req\_sketch<float> k=30',
'req\_sketch<float> k=40',
'location', 'northwest'
);

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

#td_cpp=load('../results/tdigest_rank_error-100-my_version-true_rank.tsv');
#td_cpp=load('../results/tdigest_rank_error-100-lb_ub_rank.tsv');
td100_cpp=load('../results/tdigest_rank_error-100.tsv');
#td_java=load('../../results/tdigest_rank_error-100.tsv');
#td_java=load('../../results/tdigest_rank_error-100-new.tsv');

td200_cpp=load('../results/tdigest_rank_error-200-float.tsv');
td200_n_cpp=load('../results/tdigest_rank_error-200-new_sizing.tsv');

#td_merge2_cpp=load('../results/tdigest_merge_rank_error-100-2way.tsv');
#td_merge4_cpp=load('../results/tdigest_merge_rank_error-100-4way.tsv');
#td_merge32_cpp=load('../results/tdigest_merge_rank_error-100-32way.tsv');
#td_merge64_cpp=load('../results/tdigest_merge_rank_error-100-64way.tsv');
#td_merge128_cpp=load('../results/tdigest_merge_rank_error-100-128way.tsv');

td200_merge32_cpp=load('../results/tdigest_merge_rank_error-200-32way.tsv');

#td_merge2_java=load('../../results/tdigest_merge_rank_error-100-2way.tsv');
#td_merge4_java=load('../../results/tdigest_merge_rank_error-100-4way.tsv');
#td_merge32_java=load('../../results/tdigest_merge_rank_error-100-32way.tsv');
#td_merge64_java=load('../../results/tdigest_merge_rank_error-100-64way.tsv');
#td_merge128_java=load('../../results/tdigest_merge_rank_error-100-128way.tsv');

#req12=load('../results/req_sketch_error-k12-double.tsv');
req10=load('../results/req_sketch_error-k10-double.tsv');
#req8=load('../results/req_sketch_error-k8-double.tsv');

req20=load('../results/req_sketch_error-k20-double.tsv');
req30=load('../results/req_sketch_error-k30-double.tsv');
req40=load('../results/req_sketch_error-k40-double.tsv');

hold on;

semilogx(td100_cpp(:,1), td100_cpp(:,6), 'linewidth', 2);
semilogx(td200_cpp(:,1), td200_cpp(:,6), 'linewidth', 2);
semilogx(td200_n_cpp(:,1), td200_n_cpp(:,6), 'linewidth', 2);

#semilogx(td_java(:,1), td_java(:,4), 'linewidth', 2);
#semilogx(td_java(:,1), td_java(:,5), 'linewidth', 2);
#semilogx(td_java(:,1), td_java(:,6), 'linewidth', 2);

#semilogx(td_merge32_cpp(:,1), td_merge32_cpp(:,5), 'linewidth', 2);
#semilogx(td_merge64_cpp(:,1), td_merge64_cpp(:,5), 'linewidth', 2);
#semilogx(td_merge128_cpp(:,1), td_merge128_cpp(:,5), 'linewidth', 2);

#semilogx(td_merge2_cpp(:,1), td_merge2_cpp(:,6), 'linewidth', 2);
#semilogx(td_merge4_cpp(:,1), td_merge4_cpp(:,6), 'linewidth', 2);
#semilogx(td_merge32_cpp(:,1), td_merge32_cpp(:,6), 'linewidth', 2);
#semilogx(td_merge64_cpp(:,1), td_merge64_cpp(:,6), 'linewidth', 2);
#semilogx(td_merge128_cpp(:,1), td_merge128_cpp(:,6), 'linewidth', 2);

semilogx(td200_merge32_cpp(:,1), td200_merge32_cpp(:,6), 'linewidth', 2);

#semilogx(td_merge32_java(:,1), td_merge32_java(:,5), 'linewidth', 2);
#semilogx(td_merge64_java(:,1), td_merge64_java(:,5), 'linewidth', 2);
#semilogx(td_merge128_java(:,1), td_merge128_java(:,5), 'linewidth', 2);

#semilogx(td_merge2_java(:,1), td_merge2_java(:,6), 'linewidth', 2);
#semilogx(td_merge4_java(:,1), td_merge4_java(:,6), 'linewidth', 2);
#semilogx(td_merge32_java(:,1), td_merge32_java(:,6), 'linewidth', 2);
#semilogx(td_merge64_java(:,1), td_merge64_java(:,6), 'linewidth', 2);
#semilogx(td_merge128_java(:,1), td_merge128_java(:,6), 'linewidth', 2);

semilogx(req10(:,1), req10(:,6), 'linewidth', 2);
semilogx(req20(:,1), req20(:,6), 'linewidth', 2);
semilogx(req30(:,1), req30(:,6), 'linewidth', 2);
semilogx(req40(:,1), req40(:,6), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Rank Error of TDigest and REQ sketch, uniform distribution, 1000 trials 99 pct'
xlabel 'stream size'
ylabel 'normalized rank error |true - esimate|, %'
grid minor on
legend(
#'TDigest k=100 rank 0.5 C++',
'TDigest k=100 rank 0.99',
#'t-digest k=100 rank 0.99 C++',
'TDigest k=200 rank 0.99',
'TDigest k=200 rank 0.99 new sizing',

#'t-digest k=100 2-way merge rank 0.99 C++',
#'t-digest k=100 4-way merge rank 0.99 C++',
#'t-digest k=100 32-way merge rank 0.99 C++',
#'t-digest k=100 64-way merge rank 0.95 C++',
#'t-digest k=100 128-way merge rank 0.95 C++',

'TDigest k=200 32-way merge rank 0.99 C++',

#'t-digest k=100 32-way merge rank 0.99 Java',
#'t-digest k=100 2-way merge rank 0.99 Java',
#'t-digest k=100 4-way merge rank 0.99 Java',
#'t-digest k=100 64-way merge rank 0.95 Java',
#'t-digest k=100 128-way merge rank 0.95 Java',

#'req k=12 rank 0.01 C++',
#'req k=12 rank 0.05 C++',
#'req k=12 rank 0.5 C++',
#'REQ k=12 rank 0.95',
#'REQ k=12 rank 0.99',
#'req k=10 rank 0.05 C++',
#'req k=10 rank 0.5 C++',
'REQ k=10 rank 0.99',
#'REQ k=10 rank 0.99',
'REQ k=20 rank 0.99',
'REQ k=30 rank 0.99',
'REQ k=40 rank 0.99',
'location', 'northwest'
);

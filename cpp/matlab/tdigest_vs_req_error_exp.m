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

td200=load('../results/tdigest_rank_error-200-double-exp1.5.tsv');

req40_hra=load('../results/req_sketch_error-40-double-exp1.5-hra.tsv');
req40_lra=load('../results/req_sketch_error-40-double-exp1.5-lra.tsv');

hold on;

semilogx(td200(:,1), td200(:,6), 'linewidth', 2);
semilogx(req40_hra(:,1), req40_hra(:,6), 'linewidth', 2);

#semilogx(td200(:,1), td200(:,2), 'linewidth', 2);
#semilogx(req40_lra(:,1), req40_lra(:,2), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Rank Error of TDigest and REQ sketch, exponential distribution lambda=1.5, 1000 trials 99 pct'
xlabel 'stream size'
ylabel 'normalized rank error |true - esimate|, %'
grid minor on
legend(
'TDigest k=200 rank 0.99',
'REQ k=40 HRA rank 0.99',
'location', 'northwest'
);

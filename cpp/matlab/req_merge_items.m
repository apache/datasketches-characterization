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

req_hra_java=load('../../results/req_merge_timing_hra_k12_32way.tsv');
#req_lra_java=load('../../results/req_merge_timing_lra_k12_32way.tsv');
req_hra_float_clang=load('../results/req_merge_timing_float_hra_k12_32way_clang.tsv');
#req_lra_float_clang=load('../results/req_merge_timing_float_lra_k12_32way_clang.tsv');

hold on;

semilogx(req_hra_java(:,1), req_hra_java(:,6), 'linewidth', 2);
#semilogx(req_lra_java(:,1), req_lra_java(:,6), 'linewidth', 2);
semilogx(req_hra_float_clang(:,1), req_hra_float_clang(:,6), 'linewidth', 2);
#semilogx(req_lra_float_clang(:,1), req_lra_float_clang(:,6), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Number of retained items after merge of REQ sketch'
xlabel 'stream size'
legend('req hra 12 java', 'req hra 12 float clang', 'location', 'northwest');
ylabel 'number of retained items'
grid minor on

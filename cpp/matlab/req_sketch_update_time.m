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

kll_java=load('../../results/kll_sketch_timing_k200.tsv');
req_hra_float_clang=load('../results/req_sketch_timing_float_hra_k12_clang.tsv');
req_hra_float_gcc=load('../results/req_sketch_timing_float_hra_k12_gcc10.tsv');
req_lra_float_clang=load('../results/req_sketch_timing_float_lra_k12_clang.tsv');
req_lra_float_gcc=load('../results/req_sketch_timing_float_lra_k12_gcc10.tsv');
#req_hra_float_clang_test=load('../results/req_sketch_timing_float_hra_k12_clang_test.tsv');

hold on;

semilogx(kll_java(:,1), kll_java(:,4), 'linewidth', 2);
semilogx(req_hra_float_clang(:,1), req_hra_float_clang(:,4), 'linewidth', 2);
semilogx(req_hra_float_gcc(:,1), req_hra_float_gcc(:,4), 'linewidth', 2);
semilogx(req_lra_float_clang(:,1), req_lra_float_clang(:,4), 'linewidth', 2);
semilogx(req_lra_float_gcc(:,1), req_lra_float_gcc(:,4), 'linewidth', 2);
#semilogx(req_hra_float_clang_test(:,1), req_hra_float_clang_test(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of KLL and REQ sketches'
xlabel 'stream size'
legend('kll 200 java (float)', 'req hra 12 float clang', 'req hra 12 float gcc', 'req lra 12 float clang', 'req lra 12 float gcc', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

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

kll_java=load('../../results/kll_sketch_accuracy_k200_1k_99pct.tsv');
kll_cpp=load('../results/kll_sketch_accuracy_k200_1k_99pct.tsv');

semilogx(kll_java(:,1), kll_java(:,2), 'linewidth', 2);
hold on;
semilogx(kll_cpp(:,1), kll_cpp(:,2), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Rank Error of KLL Sketch K=200 (1000 trials 99pct)'
xlabel 'stream size'
legend('Java', 'C++');
ylabel 'single-sided normalized rank error, %'
grid minor on

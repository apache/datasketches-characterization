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
cpc11=load('../results/cpc_sketch_memory_lgk11.tsv');
cpc12=load('../results/cpc_sketch_memory_lgk12.tsv');

hll4_12=load('../results/hll_sketch_memory_hll4_lgk12.tsv');
hll4_13=load('../results/hll_sketch_memory_hll4_lgk13.tsv');

#theta12=load('../results/theta_sketch_memory_lgk12.tsv');

semilogx(cpc11(:,1), cpc11(:,11), 'linewidth', 2);
hold on;
semilogx(cpc12(:,1), cpc12(:,11), 'linewidth', 2);

semilogx(hll4_12(:,1), hll4_12(:,11), 'linewidth', 2);
semilogx(hll4_13(:,1), hll4_13(:,11), 'linewidth', 2);

#semilogx(theta12(:,1), theta12(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage by CPC and HLL sketches (updates, no transients, C++)'
xlabel 'number of distinct values'
legend('CPC lgk=11', 'CPC lgk=12', 'HLL4 lgk=12', 'HLL4 lgk=13', 'location', 'northwest');
ylabel 'size in memory, bytes'
grid minor on

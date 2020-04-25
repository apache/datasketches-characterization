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
cpc4=load('../results/cpc_sketch_memory_lgk4.tsv');
cpc5=load('../results/cpc_sketch_memory_lgk5.tsv');

hll4_4=load('../results/hll_sketch_memory_hll4_lgk4.tsv');
hll4_5=load('../results/hll_sketch_memory_hll4_lgk5.tsv');

semilogx(cpc4(:,1), cpc4(:,11), 'linewidth', 2);
hold on;
semilogx(cpc5(:,1), cpc5(:,11), 'linewidth', 2);

semilogx(hll4_4(:,1), hll4_4(:,11), 'linewidth', 2);
semilogx(hll4_5(:,1), hll4_5(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage by CPC and HLL sketches (updates, no transients, C++)'
xlabel 'number of distinct values'
legend('CPC lgk=4', 'CPC lgk=5', 'HLL4 lgk=4', 'HLL4 lgk=5', 'location', 'northwest');
ylabel 'size in memory, bytes'
grid minor on

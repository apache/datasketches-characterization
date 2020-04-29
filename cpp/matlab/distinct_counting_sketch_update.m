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
#cpc10=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');
cpc12=load('../results/cpc_sketch_timing_lgk12_gcc9.tsv');

hll4_12=load('../results/hll_sketch_timing_hll4_lgk12_gcc9.tsv');

theta_12=load('../results/theta_sketch_timing_lgk12_x8_gcc9.tsv');

hold on;

#semilogx(cpc10(:,1), cpc10(:,4), 'linewidth', 2);
semilogx(cpc12(:,1), cpc12(:,4), 'linewidth', 2);
semilogx(hll4_12(:,1), hll4_12(:,4), 'linewidth', 2);

semilogx(theta_12(:,1), theta_12(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC, HLL and Theta sketches'
xlabel 'number of distinct values'
legend('CPC lgk=12 gcc9', 'HLL4 lgk=12 gcc9', 'Theta lgk=12 x8 gcc9', 'location', 'northeast');
ylabel 'update time, ns'
grid minor on

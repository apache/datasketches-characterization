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

#cpc1=load('../results/cpc_union_timing_lgk10_32way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

cpc3=load('../../results/cpc_union_update_timing_lgk12_32way.tsv');

hll4_12=load('../results/hll_union_timing_hll4_lgk12_32way_gcc9.tsv');
#hll8_12=load('../results/hll_union_timing_hll8_lgk12_32way_gcc9.tsv');

hll4_12_java=load('../../results/hll_union_update_timing_hll4_lgk12_32way.tsv');
#hll8_12_java=load('../../results/hll_union_update_timing_hll8_lgk12_32way.tsv');

theta_cpp=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc10.tsv');
theta_java=load('../../results/theta_union_update_timing_lgk12_32way.tsv');

hold on;

#semilogx(cpc1(:,1), cpc1(:,7), 'linewidth', 2);
semilogx(cpc2(:,1), cpc2(:,7), 'linewidth', 2);
semilogx(cpc3(:,1), cpc3(:,3), 'linewidth', 2);

semilogx(hll4_12(:,1), hll4_12(:,7), 'linewidth', 2);
#semilogx(hll8_12(:,1), hll8_12(:,7), 'linewidth', 2);

semilogx(hll4_12_java(:,1), hll4_12_java(:,3), 'linewidth', 2);
#semilogx(hll8_12_java(:,1), hll8_12_java(:,3), 'linewidth', 2);

semilogx(theta_cpp(:,1), theta_cpp(:,8), 'linewidth', 2);
semilogx(theta_java(:,1), theta_java(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way union'
xlabel 'number of distinct values'
legend('CPC lgk=12 gcc9', 'CPC lgk=12 Java', 'HLL4 lgk=12 gcc9', 'HLL4 lgk=12 Java', 'Theta lgk=12 compact trimmed gcc10', 'Theta lgk=12 compact trimmed Java', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

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

cpc1=load('../results/cpc_union_timing_lgk10_32way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

hll1=load('../results/hll_union_timing_lgk11_32way_gcc9.tsv');
hll2=load('../results/hll_union_timing_lgk12_32way_gcc9.tsv');

theta1=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');
theta2=load('../../results/theta_union_result_timing_lgk12_32way.tsv');

semilogx(cpc1(:,1), cpc1(:,8), 'linewidth', 2);
hold on;
semilogx(cpc2(:,1), cpc2(:,8), 'linewidth', 2);

semilogx(hll1(:,1), hll1(:,8), 'linewidth', 2);
semilogx(hll2(:,1), hll2(:,8), 'linewidth', 2);

semilogx(theta1(:,1), theta1(:,9), 'linewidth', 2);
semilogx(theta2(:,1), theta2(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Get result time of 32-way union'
xlabel 'number of distinct values'
legend('CPC lgk=10 gcc9', 'CPC lgk=12 gcc9', 'HLL lgk=11 gcc9', 'HLL lgk=12 gcc9', 'Theta lgk=12 compact trimmed gcc9', 'Theta lgk=12 compact trimmed Java', "location", 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

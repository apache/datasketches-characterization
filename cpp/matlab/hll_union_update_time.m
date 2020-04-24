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

hll4_11=load('../results/hll_union_timing_hll4_lgk11_32way_gcc9.tsv');
hll8_11=load('../results/hll_union_timing_hll8_lgk11_32way_gcc9.tsv');

hll4_11_java=load('../../results/hll_union_update_timing_hll4_lgk11_32way.tsv');
hll8_11_java=load('../../results/hll_union_update_timing_hll8_lgk11_32way.tsv');

hold on;

semilogx(hll4_11(:,1), hll4_11(:,7), 'linewidth', 2);
semilogx(hll8_11(:,1), hll8_11(:,7), 'linewidth', 2);

semilogx(hll4_11_java(:,1), hll4_11_java(:,3), 'linewidth', 2);
semilogx(hll8_11_java(:,1), hll8_11_java(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way HLL union'
xlabel 'number of distinct values'
legend('HLL4 lgk=11 gcc9', 'HLL8 lgk=11 gcc9', 'HLL4 lgk=11 Java', 'HLL8 lgk-11 Java', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

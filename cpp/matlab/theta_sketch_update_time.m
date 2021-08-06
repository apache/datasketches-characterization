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
theta_12=load('../results/theta_sketch_timing_lgk12_x8_gcc9.tsv');
tuple_12_double=load('../results/tuple_sketch_timing_double_lgk12_x8_gcc9.tsv');
aod_12=load('../results/aod_sketch_timing_lgk12_x8_gcc9.tsv');
aod_12_java=load('../../results/aod_sketch_update_timing_lgk12_x8.tsv');
#theta_x1_12=load('../results/theta_sketch_timing_lgk12_x1_gcc10.tsv');

hold on;

semilogx(theta_12(:,1), theta_12(:,4), 'linewidth', 2);
semilogx(tuple_12_double(:,1), tuple_12_double(:,4), 'linewidth', 2);
semilogx(aod_12(:,1), aod_12(:,4), 'linewidth', 2);
semilogx(aod_12_java(:,1), aod_12_java(:,3), 'linewidth', 2);
semilogx(theta_x1_12(:,1), theta_x1_12(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of Theta sketches'
xlabel 'number of distinct values'
legend('Theta lgk=12 x8 gcc9', 'Tuple double lgk=12 x8 gcc9', 'AOD lgk=12 x8 gcc9', 'AOD lgk=12 x8 Java', 'location', 'northeast');
ylabel 'update time, ns'
grid minor on

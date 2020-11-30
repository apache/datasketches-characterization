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

theta_cpp=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');
theta_cpp_exp=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9_experimental.tsv');
theta_java=load('../../results/theta_union_update_timing_lgk12_32way.tsv');
tuple_cpp=load('../results/tuple_union_timing_double_lgk12_32way_compact_trimmed_gcc9.tsv');
aod_java=load('../../results/aod_union_update_timing_lgk12_32way.tsv');
aod_cpp=load('../results/aod_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');

hold on;

semilogx(theta_cpp(:,1), theta_cpp(:,8), 'linewidth', 2);
semilogx(theta_cpp_exp(:,1), theta_cpp_exp(:,8), 'linewidth', 2);
semilogx(theta_java(:,1), theta_java(:,3), 'linewidth', 2);
semilogx(tuple_cpp(:,1), tuple_cpp(:,8), 'linewidth', 2);
semilogx(aod_java(:,1), aod_java(:,3), 'linewidth', 2);
semilogx(aod_cpp(:,1), aod_cpp(:,8), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way union'
xlabel 'number of distinct values'
legend('Theta lgk=12 compact trimmed gcc9', 'Theta experimental', 'Theta lgk=12 compact trimmed Java', 'Tuple double lgk=12 compact trimmed gcc9', 'AOD compact trimmed Java', 'AOD compact trimmed gcc9', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

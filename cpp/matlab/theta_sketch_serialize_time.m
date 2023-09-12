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

theta_12=load('../results/theta_sketch_timing_lgk12_trimmed_gcc11.tsv');
theta_12_uleb=load('../results/theta_sketch_timing_lgk12_trimmed_uleb128_gcc11.tsv');
theta_12_flz=load('../results/theta_sketch_timing_lgk12_trimmed_flz_gcc11.tsv');
theta_12_slz=load('../results/theta_sketch_timing_lgk12_trimmed_slz_gcc11.tsv');
theta_12_mlz=load('../results/theta_sketch_timing_lgk12_trimmed_mlz_gcc11.tsv');

hold on;

semilogx(theta_12(:,1), theta_12(:,6), 'linewidth', 2);
semilogx(theta_12_uleb(:,1), theta_12_uleb(:,9), 'linewidth', 2);
semilogx(theta_12_flz(:,1), theta_12_flz(:,9), 'linewidth', 2);
semilogx(theta_12_slz(:,1), theta_12_slz(:,9), 'linewidth', 2);
semilogx(theta_12_mlz(:,1), theta_12_mlz(:,6), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Serialization time of Theta sketch (lgK=12 trimmed ordered compact)'
xlabel 'number of distinct values'
#legend('Theta lgk=12 x8 gcc10', 'Tuple double lgk=12 x8 gcc9',  'location', 'northeast');
legend(
'C++ no compression',
'C++ ULEB128',
'C++ FLZ',
'C++ SLZ',
'C++ MLZ',
'location', 'northwest');
ylabel 'serialization time, ns'
grid minor on

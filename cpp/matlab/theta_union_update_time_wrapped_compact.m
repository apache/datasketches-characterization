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

theta16=load('../results/theta_union_timing_lgk16_32way_compact_trimmed_gcc10.tsv');
theta16_wrapped=load('../results/theta_union_timing_lgk16_32way_compact_trimmed_wrapped_gcc10.tsv');

hold on;

semilogx(theta16(:,1), theta16(:,8), 'linewidth', 2);

# with deserialization time added
semilogx(theta16(:,1), theta16(:,8) + theta16(:,7), 'linewidth', 2);

semilogx(theta16_wrapped(:,1), theta16_wrapped(:,8), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way union (Theta lgk=16 compact trimmed)'
xlabel 'number of distinct values'
legend('compact sketches', 'with deserialize', 'wrapped compact sketches', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

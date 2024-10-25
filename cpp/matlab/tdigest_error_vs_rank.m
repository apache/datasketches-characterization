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

td_cpp=load('../results/tdigest_error_vs_rank-k100-sl25.tsv');

hold on;

plot(td_cpp(:,1), td_cpp(:,8), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,7), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,6), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,5), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,4), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,3), 'linewidth', 2);
plot(td_cpp(:,1), td_cpp(:,2), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'TDigest rank error K=100, N=2^2^5, uniform distribution, 10000 trials'
xlabel 'normalized rank'
ylabel 'rank error'
grid minor on
legend(
'+3SD',
'+2SD',
'+1SD',
'median',
'-1SD',
'-2SD',
'-3SD',
'location', 'northwest');

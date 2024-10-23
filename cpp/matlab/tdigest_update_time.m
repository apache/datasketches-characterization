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

td100_java=load('../../results/tdigest_timing-100.tsv');
td100_dbl_gcc13=load('../results/tdigest_timing-100-double-gcc13.tsv');
td100_dbl_gcc13_res=load('../results/tdigest_timing-100-double-gcc13-reserve.tsv');

hold on;
semilogx(td100_dbl_gcc13(:,1), td100_dbl_gcc13(:,4), 'linewidth', 2);
semilogx(td100_dbl_gcc13_res(:,1), td100_dbl_gcc13_res(:,4), 'linewidth', 2);
semilogx(td100_java(:,1), td100_java(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of TDigest'
xlabel 'stream size'
legend(
'cmopression=100 double gcc13',
'cmopression=100 double gcc13 with reserve',
'compression=100 Java tdunning',
'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

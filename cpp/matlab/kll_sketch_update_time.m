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

kll_java=load('../../results/kll_sketch_timing_k200.tsv');
kll_float=load('../results/kll_sketch_timing_float_k200_gcc9.tsv');
kll_str=load('../results/kll_sketch_timing_string_k200_gcc9.tsv');

semilogx(kll_java(:,1), kll_java(:,4), 'linewidth', 2);
hold on;
semilogx(kll_float(:,1), kll_float(:,4), 'linewidth', 2);
semilogx(kll_str(:,1), kll_str(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of KLL sketch K=200'
xlabel 'stream size'
legend('java (float)', 'c++ float', 'c++ string', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

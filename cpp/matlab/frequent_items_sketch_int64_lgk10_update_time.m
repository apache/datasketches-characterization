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
d1=load('../results/frequent_items_sketch_int64_lgk10_zipf07.tsv');
d2=load('../results/frequent_items_sketch_int64_lgk10_zipf11.tsv');
d3=load('../results/frequent_items_sketch_int64_lgk10_zipf07_max.tsv');
d4=load('../results/frequent_items_sketch_int64_lgk10_zipf11_max.tsv');
semilogx([d1(:,1), d2(:,1), d3(:,1), d4(:,1)], [d1(:,4), d2(:,4), d3(:,4), d4(:,4)], 'linewidth', 2);
set(gca, 'fontsize', 16);
title 'Update time of frequent items sketch<long long>(10)'
xlabel 'n'
ylabel 'time, nanoseconds'
legend('zipf 0.7 resize', 'zipf 1.1 resize', 'zipf 0.7 max size', 'zipf 1.1 max size');
grid minor on

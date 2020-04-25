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
kll200_float=load('../results/kll_sketch_memory_float_k200.tsv');
kll200_uint64=load('../results/kll_sketch_memory_int64_k200.tsv');

semilogx(kll200_float(:,1), kll200_float(:,11), 'linewidth', 2);
hold on;
semilogx(kll200_uint64(:,1), kll200_uint64(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage by KLL sketch (updates, no transients, C++)'
xlabel 'number of values'
legend('float k=200', 'int64 k=200', 'location', 'southeast');
ylabel 'size in memory, bytes'
grid minor on

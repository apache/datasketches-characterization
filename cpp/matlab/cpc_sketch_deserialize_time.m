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
k=1024;
d=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');

for i=1:size(d, 1)
  d(i,9) = d(i,6) / min(d(i,1), k);
end
# skip a few first points to see details better
semilogx(d(12:end,1) / k, d(12:end,9), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Deserialize time of CPC sketch'
xlabel 'n / k'
legend('lgk=10 gcc 9', "location", 'northeast');
ylabel 'time / min(n, k), nanoseconds'
grid minor on

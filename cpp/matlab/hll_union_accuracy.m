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
d=load('../results/hll_union_accuracy_lgk12.tsv');

# median
h1=semilogx(d(:,1), d(:,10) * 100, 'k', 'linewidth', 2);

hold on

# 1 standard deviation
h2=semilogx(d(:,1), d(:,9) * 100, 'b', 'linewidth', 2);
h3=semilogx(d(:,1), d(:,11) * 100, 'b', 'linewidth', 2);

# 2 standard deviations
h4=semilogx(d(:,1), d(:,8) * 100, 'r', 'linewidth', 2);
h5=semilogx(d(:,1), d(:,12) * 100, 'r', 'linewidth', 2);

# 3 standard deviations
h6=semilogx(d(:,1), d(:,7) * 100, 'g', 'linewidth', 2);
h7=semilogx(d(:,1), d(:,13) * 100, 'g', 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Relative Error of HLL union lgk=12 (512 trials)'
xlabel 'number of distinct values'
legend([h1, h2, h4, h6], 'median', '67% interval', '95% interval', '99% interval');
ylabel 'relative error, %'
grid minor on

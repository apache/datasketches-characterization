clf;

cpc1=load('../results/cpc_union_timing_lgk10_32way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

hll1=load('../results/hll_union_timing_lgk11_32way_gcc9.tsv');
hll2=load('../results/hll_union_timing_lgk12_32way_gcc9.tsv');

theta1=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');
theta2=load('../../results/theta_union_result_timing_lgk12_32way.tsv');

semilogx(cpc1(:,1), cpc1(:,8), 'linewidth', 2);
hold on;
semilogx(cpc2(:,1), cpc2(:,8), 'linewidth', 2);

semilogx(hll1(:,1), hll1(:,8), 'linewidth', 2);
semilogx(hll2(:,1), hll2(:,8), 'linewidth', 2);

semilogx(theta1(:,1), theta1(:,9), 'linewidth', 2);
semilogx(theta2(:,1), theta2(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Get result time of 32-way union'
xlabel 'number of distinct values'
legend('CPC lgk=10 gcc9', 'CPC lgk=12 gcc9', 'HLL lgk=11 gcc9', 'HLL lgk=12 gcc9', 'Theta lgk=12 compact trimmed gcc9', 'Theta lgk=12 compact trimmed Java', "location", 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

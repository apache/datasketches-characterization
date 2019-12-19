clf;

cpc1=load('../results/cpc_union_timing_lgk10_32way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

hll1=load('../results/hll_union_timing_lgk11_32way_gcc9.tsv');
hll2=load('../results/hll_union_timing_lgk12_32way_gcc9.tsv');

theta1=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');
#theta2=load('../results/theta_union_timing_lgk12_32way_compact_gcc9.tsv');
#theta3=load('../results/theta_union_timing_lgk12_32way_compact_unordered_gcc9.tsv');
#theta4=load('../results/theta_union_timing_lgk12_32way_compact_unordered_trimmed_gcc9.tsv');

semilogx(cpc1(:,1) / 1024, cpc1(:,6) + cpc1(:,7), 'linewidth', 2);
hold on;
semilogx(cpc2(:,1) / 4096, cpc2(:,6) + cpc2(:,7), 'linewidth', 2);

semilogx(hll1(:,1) / 2048, hll1(:,6) + hll1(:,7), 'linewidth', 2);
semilogx(hll2(:,1) / 4096, hll2(:,6) + hll2(:,7), 'linewidth', 2);

semilogx(theta1(:,1) / 4096, theta1(:,7) + theta1(:,8), 'linewidth', 2);
#semilogx(theta2(:,1) / 4096, theta2(:,6), 'linewidth', 2);
#semilogx(theta3(:,1) / 4096, theta3(:,6), 'linewidth', 2);
#semilogx(theta4(:,1) / 4096, theta4(:,6), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way union with deserialization'
xlabel 'n / k'
legend('CPC lgk=10', 'CPC lgk=12', 'HLL4 lgk=11 compact', 'HLL4 lgk=12 compact', 'Theta lgk=12 compact trimmed', "location", 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

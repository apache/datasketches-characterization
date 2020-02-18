clf;

#cpc1=load('../results/cpc_union_timing_lgk10_32way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

cpc3=load('../../results/cpc_union_update_timing_lgk12_32way.tsv');

hll4_12=load('../results/hll_union_timing_hll4_lgk12_32way_gcc9.tsv');
hll8_12=load('../results/hll_union_timing_hll8_lgk12_32way_gcc9.tsv');

hll4_12_java=load('../../results/hll_union_update_timing_hll4_lgk12_32way.tsv');
hll8_12_java=load('../../results/hll_union_update_timing_hll8_lgk12_32way.tsv');

theta1=load('../results/theta_union_timing_lgk12_32way_compact_trimmed_gcc9.tsv');
#theta2=load('../results/theta_union_timing_lgk12_32way_compact_gcc9.tsv');
#theta3=load('../results/theta_union_timing_lgk12_32way_compact_unordered_gcc9.tsv');
#theta4=load('../results/theta_union_timing_lgk12_32way_compact_unordered_trimmed_gcc9.tsv');

theta5=load('../../results/theta_union_update_timing_lgk12_32way.tsv');

hold on;

#semilogx(cpc1(:,1), cpc1(:,7), 'linewidth', 2);
semilogx(cpc2(:,1), cpc2(:,7), 'linewidth', 2);
semilogx(cpc3(:,1), cpc3(:,3), 'linewidth', 2);

semilogx(hll4_12(:,1), hll4_12(:,7), 'linewidth', 2);
semilogx(hll8_12(:,1), hll8_12(:,7), 'linewidth', 2);

semilogx(hll4_12_java(:,1), hll4_12_java(:,3), 'linewidth', 2);
semilogx(hll8_12_java(:,1), hll8_12_java(:,3), 'linewidth', 2);

semilogx(theta1(:,1), theta1(:,8), 'linewidth', 2);
#semilogx(theta2(:,1), theta2(:,6), 'linewidth', 2);
#semilogx(theta3(:,1), theta3(:,6), 'linewidth', 2);
#semilogx(theta4(:,1), theta4(:,6), 'linewidth', 2);

semilogx(theta5(:,1), theta5(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way union'
xlabel 'number of distinct values'
legend('CPC lgk=12 gcc9', 'CPC lgk=12 Java', 'HLL4 lgk=12 gcc9', 'HLL8 lgk=12 gcc9', 'HLL4 lgk=12 Java', 'HLL8 lgk-12 Java', 'Theta lgk=12 compact trimmed', 'Theta lgk=12 compact trimmed Java', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

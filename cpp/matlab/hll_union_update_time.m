clf;

hll4_11=load('../results/hll_union_timing_hll4_lgk11_32way_gcc9.tsv');
hll8_11=load('../results/hll_union_timing_hll8_lgk11_32way_gcc9.tsv');

hll4_11_java=load('../../results/hll_union_update_timing_hll4_lgk11_32way.tsv');
hll8_11_java=load('../../results/hll_union_update_timing_hll8_lgk11_32way.tsv');

hold on;

semilogx(hll4_11(:,1), hll4_11(:,7), 'linewidth', 2);
semilogx(hll8_11(:,1), hll8_11(:,7), 'linewidth', 2);

semilogx(hll4_11_java(:,1), hll4_11_java(:,3), 'linewidth', 2);
semilogx(hll8_11_java(:,1), hll8_11_java(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of 32-way HLL union'
xlabel 'number of distinct values'
legend('HLL4 lgk=11 gcc9', 'HLL8 lgk=11 gcc9', 'HLL4 lgk=11 Java', 'HLL8 lgk-11 Java', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

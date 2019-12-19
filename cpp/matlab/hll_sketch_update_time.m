clf;
d1=load('../results/hll_sketch_timing_hll4_lgk12_gcc9.tsv');

semilogx(d1(:,1), d1(:, 4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of HLL sketch'
xlabel 'number of distinct values'
legend('HLL4 lgk=12 GCC9', "location", 'northeast');
ylabel 'update time, nanoseconds'
grid minor on

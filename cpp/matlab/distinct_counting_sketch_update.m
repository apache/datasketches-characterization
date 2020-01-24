clf;
#cpc10=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');
cpc12=load('../results/cpc_sketch_timing_lgk12_gcc9.tsv');

hll4_12=load('../results/hll_sketch_timing_hll4_lgk12_gcc9.tsv');

theta_12=load('../results/theta_sketch_timing_lgk12_x8_gcc9.tsv');

#semilogx(cpc10(:,1), cpc10(:,4), 'linewidth', 2);
#hold on;
semilogx(cpc12(:,1), cpc12(:,4), 'linewidth', 2);
hold on;
semilogx(hll4_12(:,1), hll4_12(:,4), 'linewidth', 2);

semilogx(theta_12(:,1), theta_12(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC, HLL and Theta sketches'
xlabel 'number of distinct values'
legend('CPC lgk=12 gcc9', 'HLL4 lgk=12 gcc9', 'Theta lgk=12 x8 gcc9', "location", 'northeast');
ylabel 'update time, ns'
grid minor on

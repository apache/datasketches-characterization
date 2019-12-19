clf;
cpc10=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');
cpc12=load('../results/cpc_sketch_timing_lgk12_gcc9.tsv');

hll4_12=load('../results/hll_sketch_timing_hll4_lgk12_gcc9.tsv');

semilogx(cpc10(:,1), cpc10(:,4), 'linewidth', 2);
hold on;
semilogx(cpc12(:,1), cpc12(:,4), 'linewidth', 2);

semilogx(hll4_12(:,1), hll4_12(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC and HLL sketches'
xlabel 'number of distinct values'
legend('CPC lgk=10 gcc 9', 'CPC lgk=12 gcc 9', 'HLL4 lgk=12 gcc 9', "location", 'northeast');
ylabel 'update time, ns'
grid minor on

clf;
cpc10=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');
cpc12=load('../results/cpc_sketch_timing_lgk12_gcc9.tsv');

semilogx(cpc10(:,1), cpc10(:,4), 'linewidth', 2);
hold on;
semilogx(cpc12(:,1), cpc12(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC sketch'
xlabel 'number of distinct values'
legend('lgk=10 gcc9', 'lgk=12 gcc9', 'location', 'northeast');
ylabel 'update time, nanoseconds'
grid minor on

clf;

cpc1=load('../results/cpc_union_timing_lgk10_8way_gcc9.tsv');
cpc2=load('../results/cpc_union_timing_lgk10_16way_gcc9.tsv');
cpc3=load('../results/cpc_union_timing_lgk12_16way_gcc9.tsv');
cpc4=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

cpc5=load('../../results/cpc_union_timing_lgk12_32way.tsv');

semilogx(cpc1(:,1) / 1024, cpc1(:,5), 'linewidth', 2);
hold on;
semilogx(cpc2(:,1) / 1024, cpc2(:,5), 'linewidth', 2);
semilogx(cpc3(:,1) / 4096, cpc3(:,5), 'linewidth', 2);

semilogx(cpc4(:,1) / 4096, cpc4(:,7), 'linewidth', 2);

semilogx(cpc5(:,1) / 4096, cpc5(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC union'
xlabel 'n / k'
legend('CPC lgk=10 8-way', 'CPC lgk=10 16-way', 'CPC lgk=12 16-way', 'CPC lgk=12 32-way', 'CPC lgk=12 32-way Java', 'location', 'northeast');
ylabel 'update time, nanoseconds'
grid minor on

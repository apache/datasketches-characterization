clf;

cpc10_8=load('../results/cpc_union_timing_lgk10_8way_gcc9.tsv');
cpc10_16=load('../results/cpc_union_timing_lgk10_16way_gcc9.tsv');
cpc12_16=load('../results/cpc_union_timing_lgk12_16way_gcc9.tsv');
cpc12_32=load('../results/cpc_union_timing_lgk12_32way_gcc9.tsv');

cpc12_32_java=load('../../results/cpc_union_update_timing_lgk12_32way.tsv');

hold on;
semilogx(cpc10_8(:,1), cpc10_8(:,5), 'linewidth', 2);
semilogx(cpc10_16(:,1), cpc10_16(:,5), 'linewidth', 2);
semilogx(cpc12_16(:,1), cpc12_16(:,5), 'linewidth', 2);
semilogx(cpc12_32(:,1), cpc12_32(:,7), 'linewidth', 2);

semilogx(cpc12_32_java(:,1), cpc12_32_java(:,3), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of CPC union'
xlabel 'number of distinct values'
legend('CPC lgk=10 8-way gcc9', 'CPC lgk=10 16-way gcc9', 'CPC lgk=12 16-way gcc9', 'CPC lgk=12 32-way gcc9', 'CPC lgk=12 32-way Java', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

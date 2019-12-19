clf;
k=1024;
d=load('../results/cpc_sketch_timing_lgk10_gcc9.tsv');

for i=1:size(d, 1)
  d(i,9) = d(i,6) / min(d(i,1), k);
end
# skip a few first points to see details better
semilogx(d(12:end,1) / k, d(12:end,9), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Deserialize time of CPC sketch'
xlabel 'n / k'
legend('lgk=10 gcc 9', "location", 'northeast');
ylabel 'time / min(n, k), nanoseconds'
grid minor on

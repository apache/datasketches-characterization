clf;
kll200=load('../results/kll_sketch_memory_k200.tsv');

semilogx(kll200(:,1), kll200(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage by KLL sketch (updates, no transients, C++)'
xlabel 'number of values'
legend('uint64 k=200', "location", 'southeast');
ylabel 'size in memory, bytes'
grid minor on

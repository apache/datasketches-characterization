clf;
kll200_float=load('../results/kll_sketch_memory_float_k200.tsv');
kll200_uint64=load('../results/kll_sketch_memory_int64_k200.tsv');

semilogx(kll200_float(:,1), kll200_float(:,11), 'linewidth', 2);
hold on;
semilogx(kll200_uint64(:,1), kll200_uint64(:,11), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Memory usage by KLL sketch (updates, no transients, C++)'
xlabel 'number of values'
legend('float k=200', 'int64 k=200', 'location', 'southeast');
ylabel 'size in memory, bytes'
grid minor on

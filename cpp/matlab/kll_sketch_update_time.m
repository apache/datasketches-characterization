clf;

kll_java=load('../../results/kll_sketch_timing_k200.tsv');
kll_float=load('../results/kll_sketch_timing_float_k200.tsv');
kll_str=load('../results/kll_sketch_timing_string_k200.tsv');

semilogx(kll_java(:,1), kll_java(:,4), 'linewidth', 2);
hold on;
semilogx(kll_float(:,1), kll_float(:,4), 'linewidth', 2);
semilogx(kll_str(:,1), kll_str(:,4), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Update time of KLL sketch K=200'
xlabel 'stream size'
legend('java', 'c++ float', 'c++ string', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

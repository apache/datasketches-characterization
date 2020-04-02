clf;

#kll_java=load('../../results/kll_sketch_timing_k200.tsv');
kll_float=load('../results/kll_merge_timing_float_k200_gcc9.tsv');
kll_float_move=load('../results/kll_merge_timing_float_k200_move_gcc9.tsv');
kll_str=load('../results/kll_merge_timing_string_k200_gcc9.tsv');
kll_str_move=load('../results/kll_merge_timing_string_k200_move_gcc9.tsv');

hold on;
#semilogx(kll_java(:,1), kll_java(:,4), 'linewidth', 2);
semilogx(kll_float(:,1), kll_float(:,5), 'linewidth', 2);
semilogx(kll_float_move(:,1), kll_float_move(:,5), 'linewidth', 2);
semilogx(kll_str(:,1), kll_str(:,5), 'linewidth', 2);
semilogx(kll_str_move(:,1), kll_str_move(:,5), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Merge time of KLL sketch K=200'
xlabel 'stream size'
legend('c++ float', 'c++ float move', 'c++ string copy', 'c++ string move', 'location', 'northwest');
ylabel 'update time, nanoseconds'
grid minor on

clf;
k=1024;
d1=load('../results/frequent_items_sketch_int64_lgk10_zipf07.tsv');
d2=load('../results/frequent_items_sketch_int64_lgk10_zipf11.tsv');
d3=load('../results/frequent_items_sketch_int64_lgk10_zipf07_max.tsv');
d4=load('../results/frequent_items_sketch_int64_lgk10_zipf11_max.tsv');
semilogx([d1(:,1) / k, d2(:,1) / k, d3(:,1) / k, d4(:,1) / k], [d1(:,4), d2(:,4), d3(:,4), d4(:,4)], 'linewidth', 2);
set(gca, 'fontsize', 16);
title 'Update time of frequent items sketch<long long>(10)'
xlabel 'n / k'
ylabel 'time, nanoseconds'
legend('zipf 0.7 resize', 'zipf 1.1 resize', 'zipf 0.7 max size', 'zipf 1.1 max size');
grid minor on

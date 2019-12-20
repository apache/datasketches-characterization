clf;
k=1024;
d1=load('../results/frequent_items_sketch_int64_lgk10_zipf07.tsv');
d2=load('../results/frequent_items_sketch_int64_lgk10_zipf1.tsv');
d3=load('../results/frequent_items_sketch_int64_lgk10_zipf11.tsv');
d4=load('../results/frequent_items_sketch_int64_lgk10_geom0005.tsv');
semilogx([d1(:,1) / k, d2(:,1) / k, d3(:,1) / k, d4(:,1) / k], [d1(:,9), d2(:,9), d3(:,9), d4(:,9)], 'linewidth', 2);

# substitute zeros for loglog plot
d1(d1==0)=1e-4;
d2(d2==0)=1e-4;
d3(d3==0)=1e-4;
d4(d4==0)=1e-4;
#loglog([d1(:,1) / k, d2(:,1) / k, d3(:,1) / k, d4(:,1) / k], [d1(:,9), d2(:,9), d3(:,9), d4(:,9)], 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Maximum error of frequent items sketch<long long>(10)'
xlabel 'n / k'
ylabel 'max error'
legend('zipf 0.7', 'zipf 1.0', 'zipf 1.1', 'geom 0.005');
grid minor on

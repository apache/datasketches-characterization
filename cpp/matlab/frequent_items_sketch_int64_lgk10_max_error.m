clf;
d1=load('../results/frequent_items_sketch_int64_lgk10_zipf07.tsv');
d2=load('../results/frequent_items_sketch_int64_lgk10_zipf1.tsv');
d3=load('../results/frequent_items_sketch_int64_lgk10_zipf11.tsv');
d4=load('../results/frequent_items_sketch_int64_lgk10_geom0005.tsv');
semilogx([d1(:,1), d2(:,1), d3(:,1), d4(:,1)], [d1(:,9), d2(:,9), d3(:,9), d4(:,9)], 'linewidth', 2);

# substitute zeros for loglog plot
d1(d1==0)=1e-3;
d2(d2==0)=1e-3;
d3(d3==0)=1e-3;
d4(d4==0)=1e-3;
#loglog([d1(:,1), d2(:,1), d3(:,1), d4(:,1)], [d1(:,9), d2(:,9), d3(:,9), d4(:,9)], 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Maximum error of frequent items sketch<long long>(10)'
xlabel 'n'
ylabel 'max error'
legend('zipf 0.7', 'zipf 1.0', 'zipf 1.1', 'geom 0.005', 'location', 'northwest');
grid minor on

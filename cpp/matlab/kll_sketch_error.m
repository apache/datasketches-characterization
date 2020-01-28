clf;

kll_java=load('../../results/kll_sketch_accuracy_k200_1k_99pct.tsv');
kll_cpp=load('../results/kll_sketch_accuracy_k200_1k_99pct.tsv');

#semilogx([kll1(:,1), kll2(:,1), kll3(:,1)], [kll1(:,2), kll2(:,2), kll3(:,2)], 'linewidth', 2);

semilogx(kll_java(:,1), kll_java(:,2), 'linewidth', 2);
hold on;
semilogx(kll_cpp(:,1), kll_cpp(:,2), 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Rank Error of KLL Sketch K=200 (99pct)'
xlabel 'stream size'
legend('Java', 'C++');
ylabel 'single-sided normalized rank error, %'
grid minor on

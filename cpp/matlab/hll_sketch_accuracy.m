clf;
d=load('../results/hll_sketch_accuracy_lgk12.tsv');

# median
h1=semilogx(d(:,1), d(:,10) * 100, 'k', 'linewidth', 2);

hold on

# 1 standard deviation
h2=semilogx(d(:,1), d(:,9) * 100, 'b', 'linewidth', 2);
h3=semilogx(d(:,1), d(:,11) * 100, 'b', 'linewidth', 2);

# 2 standard deviations
h4=semilogx(d(:,1), d(:,8) * 100, 'r', 'linewidth', 2);
h5=semilogx(d(:,1), d(:,12) * 100, 'r', 'linewidth', 2);

# 3 standard deviations
h6=semilogx(d(:,1), d(:,7) * 100, 'g', 'linewidth', 2);
h7=semilogx(d(:,1), d(:,13) * 100, 'g', 'linewidth', 2);

set(gca, 'fontsize', 16);
title 'Relative Error of HLL Sketch lgk=12 (32768 trials)'
xlabel 'number of distinct values'
legend([h1, h2, h4, h6], 'median', '67% interval', '95% interval', '99% interval');
ylabel 'relative error, %'
grid minor on

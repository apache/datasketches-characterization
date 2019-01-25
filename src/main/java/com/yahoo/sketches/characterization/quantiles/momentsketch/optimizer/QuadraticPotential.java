package com.yahoo.sketches.characterization.quantiles.momentsketch.optimizer;

//CHECKSTYLE.OFF: FinalLocalVariable
//CHECKSTYLE.OFF: JavadocMethod
//CHECKSTYLE.OFF: LineLength
//CHECKSTYLE.OFF: OperatorWrap
//CHECKSTYLE.OFF: NonEmptyAtclauseDescription
//CHECKSTYLE.OFF: JavadocParagraph
//CHECKSTYLE.OFF: WhitespaceAround
//CHECKSTYLE.OFF: EmptyLineSeparator
//CHECKSTYLE.OFF: CommentsIndentation

/**
 * Simple quadratic function for use in tests.
 */
public class QuadraticPotential implements FunctionWithHessian {
    private int k;
    private double Pval;
    private double[] Pgrad;
    private double[][] Phess;

    public QuadraticPotential(int k) {
        this.k = k;
        Pgrad = new double[k];
        Phess = new double[k][k];
    }

    @Override
    public void computeOnlyValue(double[] point, double tol) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += point[i] * point[i];
        }
        Pval = sum;
    }

    @Override
    public void computeAll(double[] point, double tol) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += point[i] * point[i];
        }
        Pval = sum;

        for (int i = 0; i < point.length; i++) {
            Pgrad[i] = 2*point[i];
            for (int j = 0; j < point.length; j++) {
                if (j == i) {
                    Phess[i][j] = 2;
                } else {
                    Phess[i][j] = 0.0;
                }
            }
        }
    }

    @Override
    public int dim() {
        return k;
    }

    @Override
    public double getValue() {
        return Pval;
    }

    @Override
    public double[] getGradient() {
        return Pgrad;
    }

    @Override
    public double[][] getHessian() {
        return Phess;
    }
}

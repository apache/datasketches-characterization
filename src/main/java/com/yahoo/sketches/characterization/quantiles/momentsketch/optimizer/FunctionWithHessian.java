package com.yahoo.sketches.characterization.quantiles.momentsketch.optimizer;

//CHECKSTYLE.OFF: FinalLocalVariable
//CHECKSTYLE.OFF: JavadocMethod
//CHECKSTYLE.OFF: LineLength
//CHECKSTYLE.OFF: OperatorWrap
//CHECKSTYLE.OFF: NonEmptyAtclauseDescription
//CHECKSTYLE.OFF: JavadocParagraph
//CHECKSTYLE.OFF: WhitespaceAround
//CHECKSTYLE.OFF: EmptyLineSeparator

/**
 * Describes a function which can be optimized using Newton's method.
 */
public interface FunctionWithHessian {
  void computeOnlyValue(double[] point, double tol);

  void computeAll(double[] point, double tol);

  int dim();

  double getValue();

  double[] getGradient();

  // Returns in row-major order
  double[][] getHessian();
}

//
// Created by Charlie Dickens on 28/06/2023.
//

#ifndef THETA_SKETCH_JACCARD_ESTIMATION_HPP
#define THETA_SKETCH_JACCARD_ESTIMATION_HPP

#include <kll_sketch.hpp>
#include <vector>
#include "jaccard_accuracy_profile.hpp"
#include "distinct_count_accuracy_profile.hpp" // To pull in the accuracy_stats object


namespace datasketches {

class accuracy_stats_double {
  public:
    accuracy_stats_double(size_t k, double true_value, size_t n, size_t union_size, size_t intersection_size);
    void update(double jaccard_estimate, double intersection_estimate);
    double get_true_value() const;
    double get_mean_est() const;
    double get_mean_rel_err() const;
    double get_rms_rel_err() const;
    size_t get_count() const;
    size_t get_stream_cardinality() const;
    size_t get_union_size() const ;
    size_t get_intersection_size() const ;
    std::vector<double> get_quantiles(const double* fractions, size_t size) const;
    std::vector<double> get_intersection_quantiles(const double* fractions, size_t size) const;
    void print_kll() const ;
    kll_sketch<double> rel_err_distribution;
    kll_sketch<double> intersection_error_distribution;

private:
    double true_value; // Jaccard index --> What we want to estimate
    size_t stream_cardinality ;
    size_t union_size ;
    size_t intersection_size ;
    double sum_est;
    double sum_rel_err;
    double sum_sq_rel_err;
    size_t count; // number of trials completed -- kept for consistency.

  };

  //class theta_sketch_jaccard_profile: public jaccard_accuracy_profile {
  class theta_sketch_jaccard_profile: public job_profile {
  public:
    void run();
  protected:
    //std::vector<accuracy_stats> jaccard_stats ;
    std::vector<accuracy_stats_double> jaccard_stats ;
  private:
    void print_stats() const;
    void write_stats() const;
  };

}


#endif //THETA_SKETCH_JACCARD_ESTIMATION_HPP

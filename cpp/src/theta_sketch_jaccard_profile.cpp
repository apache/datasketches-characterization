//
// Created by Charlie Dickens on 28/06/2023.
//
#include <cmath>
#include <theta_sketch.hpp>
#include <theta_jaccard_similarity.hpp>
#include <theta_intersection.hpp>
#include "theta_sketch_jaccard_profile.hpp"
#include <fstream>

namespace datasketches {

accuracy_stats_double::accuracy_stats_double(size_t k, double true_value, size_t n, size_t union_size, size_t intersection_size):
  true_value(true_value),
    stream_cardinality(n),
    union_size(union_size),
    intersection_size(intersection_size),
    sum_est(0),
    sum_rel_err(0),
    sum_sq_rel_err(0),
  count(0),
  rel_err_distribution(k),
  intersection_error_distribution(k)
{}

void accuracy_stats_double::update(double jaccard_estimate, double intersection_estimate) {
  sum_est += jaccard_estimate;
  const double relative_error = jaccard_estimate / true_value - 1.0;
  sum_rel_err += relative_error;
  sum_sq_rel_err += relative_error * relative_error;
  rel_err_distribution.update(relative_error);

  //Intersection updates
  const double intersection_relative_error = intersection_estimate / intersection_size - 1.0;
  intersection_error_distribution.update(relative_error);
  count++;
}

double accuracy_stats_double::get_true_value() const {
  return true_value;
}

double accuracy_stats_double::get_mean_est() const {
  return sum_est / count;
}

double accuracy_stats_double::get_mean_rel_err() const {
  return sum_rel_err / count;
}

double accuracy_stats_double::get_rms_rel_err() const {
  return sqrt(sum_sq_rel_err / count);
}

size_t accuracy_stats_double::get_count() const {
  return count;
}

size_t accuracy_stats_double::get_stream_cardinality() const {
    return stream_cardinality;
}

size_t accuracy_stats_double::get_union_size() const {
  return union_size;
}

size_t accuracy_stats_double::get_intersection_size() const {
  return intersection_size;
}

std::vector<double> accuracy_stats_double::get_quantiles(
  const double* fractions, size_t size) const {
  return rel_err_distribution.get_quantiles(fractions, size);
}

std::vector<double> accuracy_stats_double::get_intersection_quantiles(
  const double* fractions, size_t size) const {
  return intersection_error_distribution.get_quantiles(fractions, size);
}

void theta_sketch_jaccard_profile::run(){

  // Generic setup
  const size_t num_trials = 128 ;
  const size_t lg_stream_length = 23 ;
  const size_t stream_length = 1<<lg_stream_length ;
  const size_t lg_min_overlap_length = 0 ;
  const size_t lg_max_overlap_length = lg_stream_length  ;
  const size_t overlaps_ppo =  3;
  double jaccard_index ;
  const size_t quantiles_k = 10000;

  // Dataset setup
  uint64_t counter = 35538947;
  const uint64_t golden64 = 0x9e3779b97f4a7c13ULL;  // the golden ratio
  std::vector<uint64_t> intersection_sizes ;

  // Sketch setup
  const size_t lg_k = 14;
  const size_t num_test_points = count_points(lg_min_overlap_length, lg_max_overlap_length, overlaps_ppo);// intersection_sizes.size() ;  //
  size_t num_intersection = 1 << lg_min_overlap_length; // intersection_sizes.size() ; //

  // Initialize the statistics objects: -1 is to avoid going into next pwer of 2
  for (size_t i=0; i < num_test_points-1; i++) {
    num_intersection = pwr_2_law_next(overlaps_ppo, num_intersection);
    std::cout << num_test_points << " " << num_intersection << std::endl;
    intersection_sizes.push_back(num_intersection) ;

    size_t union_size = 2 * stream_length - intersection_sizes[i]; // |A| + |B| - |A n B| and |A| == |B| == stream_length
    jaccard_index = (double) intersection_sizes[i] / union_size;
    jaccard_stats.push_back(accuracy_stats_double(quantiles_k, jaccard_index, stream_length, union_size, intersection_sizes[i]));
  }

  // Final num_overlap goes into next octave so ignore.
  for (size_t i=0; i < num_test_points-1; i++){
    std::cout << "Test point " << i << " out of " << num_test_points << "." << std::endl;
    std::cout << "Intersection size:\t" << intersection_sizes[i] << std::endl;

    for (int trial=0; trial < num_trials; trial++){
      auto sk_a = update_theta_sketch::builder().set_lg_k(lg_k).build();
      auto sk_b = update_theta_sketch::builder().set_lg_k(lg_k).build();
      theta_intersection sk_intersection;

      // Add the intersecting points to both sketches.
      for (size_t intersector = 0;  intersector<intersection_sizes[i] ; intersector++) {
        sk_a.update(counter);
        sk_b.update(counter);
        counter += golden64 ;
      }

      // Add remaining number of distinct items to each sketch separately
      size_t items_remaining = stream_length - intersection_sizes[i] ;
      for (size_t distinct_item=0; distinct_item<items_remaining; distinct_item++){
        // Increment counter twice so each sketch sees a different item
        sk_a.update(counter) ;
        counter += golden64 ;
        sk_b.update(counter) ;
        counter += golden64 ;
      }

      // trim the sketches to exactly lgk items
      sk_a.trim() ;
      sk_b.trim() ;


      auto jaccard_estimate = theta_jaccard_similarity::jaccard(sk_a, sk_b);
      sk_intersection.update(sk_a);
      sk_intersection.update(sk_b);
      compact_theta_sketch intersection_result = sk_intersection.get_result();
      jaccard_stats[i].update( jaccard_estimate[1], intersection_result.get_estimate())  ;
    } // End single trial
} // End iteration space

print_stats();
write_stats() ;
}// End function

void theta_sketch_jaccard_profile::print_stats() const {
  std::cout << std::setw(12) << "n"
            << std::setw(12) << "union"
            << std::setw(12) << "intersection"
            << std::setw(12) << "jaccard"
            << std::setw(12) << "trials"
            << std::setw(12) << "mean"
            << std::setw(12) << "mean relative error"
            << std::setw(12) << "min"
            << std::setw(12) << "m3sd"
            << std::setw(12) << "m2sd"
            << std::setw(12) << "m1sd"
            << std::setw(12) << "median"
            << std::setw(12) << "p1sd"
            << std::setw(12) << "p2sd"
            << std::setw(12) << "p3sd"
            << std::setw(12) << "max"
    << std::setw(12) << "inter_min"
    << std::setw(12) << "inter_m3sd"
    << std::setw(12) << "inter_m2sd"
    << std::setw(12) << "inter_m1sd"
    << std::setw(12) << "inter_median"
    << std::setw(12) << "inter_p1sd"
    << std::setw(12) << "inter_p2sd"
    << std::setw(12) << "inter_p3sd"
    << std::setw(12) << "inter_max"
    << std::endl;

  for (const auto &stat: jaccard_stats) {
    std::cout << std::setw(12) << stat.get_stream_cardinality() << "\t";
    std::cout << std::setw(12) << stat.get_union_size() << "\t";
    std::cout << std::setw(12) << stat.get_intersection_size() << "\t";
    std::cout << std::setw(12) << stat.get_true_value() << "\t";
    std::cout << std::setw(12) << stat.get_count() << "\t"; // This is the number of trials completed.
    std::cout << std::setw(12) << stat.get_mean_est() << "\t";
    std::cout << std::setw(12) << stat.get_mean_rel_err() << "\t";
    const auto quants = stat.get_quantiles(FRACTIONS, FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = quants[i];
      std::cout << std::setw(12) << quantile << "\t";
    }
    const auto intersection_quants = stat.get_intersection_quantiles(FRACTIONS, FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = intersection_quants[i];
      std::cout << std::setw(12) << quantile << "\t";
      if (i != FRACT_LEN - 1) std::cout << "\t";
    }
    std::cout << std::endl;
  }
} // End print_stats()

void theta_sketch_jaccard_profile::write_stats() const {
  std::ofstream out_file("jaccard_accuracy_theta.tsv");
  std::string columns[] = {"n", "union", "intersection", "jaccard", "trials", "mean estimate", "mean relative error",
                           "min", "m3sd", "m2sd", "m1sd", "median", "p1sd", "p2sd", "p3sd", "max",
                           "inter_min", "inter_m3sd", "inter_m2sd", "inter_m1sd", "inter_median",
                           "inter_p1sd", "inter_p2sd", "inter_p3sd", "inter_max"} ;
  size_t arr_length(0) ;
  for (const std::string &s : columns) arr_length++ ;
  for (size_t i=0; i < arr_length; ++i){
    const std::string col_header = columns[i] ;
    out_file << col_header ;
    if (i != arr_length - 1) out_file << "\t";
  }

  out_file << std::endl;
  for (const auto &stat: jaccard_stats) {
    out_file << stat.get_stream_cardinality() << "\t";
    out_file << stat.get_union_size() << "\t";
    out_file << stat.get_intersection_size() << "\t";
    out_file << stat.get_true_value() << "\t";
    out_file << stat.get_count() << "\t"; // This is the number of trials completed.
    out_file << stat.get_mean_est() << "\t";
    out_file << stat.get_mean_rel_err() << "\t";
    const auto quants = stat.get_quantiles(FRACTIONS, FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = quants[i];
      out_file << quantile << "\t" ;
    }
    const auto intersection_quants = stat.get_intersection_quantiles(FRACTIONS, FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = intersection_quants[i];
      out_file << quantile;
      if (i != FRACT_LEN - 1) out_file << "\t";
    }
    out_file << std::endl;
  }
}// End write_stats()
} // End namespace
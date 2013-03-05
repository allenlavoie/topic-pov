/* Find the probability of the observed data ("words" in many topic
   models), integrating out topic and POV assignments. For details,
   see:
   
   Murray, I., and Salakhutdinov, R. 2009. Evaluating probabilities
   under high-dimensional latent variable models. Advances in Neural
   Information Processing Systems 21. 1137–1144. 

   Useful for estimating the number of topics and/or POVs per topic to
   use when modeling a given dataset. */

#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"
#include "sample.h"

double sum_logs(double log_a, double log_b) {
  if (log_a > log_b) {
    return log_a + log(1.0 + exp(log_b - log_a));
  } else {
    return log_b + log(1.0 + exp(log_a - log_b));
  }
}

int main(int argc, char **argv) {
  if (argc != 6) {
    printf("Usage: %s mmap_directory trials iterations_per_trial maximization_iterations threads\n",
           argv[0]);
    exit(1);
  }
  // Nothing we do will be committed back to the files
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  int trials = atoi(argv[2]);
  int do_iterations = atoi(argv[3]);
  int num_threads = atoi(argv[5]);
  int maximization_iterations = atoi(argv[4]);

  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);

  // First we need a high-probability estimate z*
  for (int i = 0; i < maximization_iterations; ++i) {
    resample_maximize(&sample_threads);
  }
  double maximized_likelihood = parallel_log_likelihood(&sample_threads);

  // Copy out the z* assignments for reference
  struct revision_assignment* max_assignments 
    = copy_revision_assignments(&mmap_info);
  for (int trial_number = 0; trial_number < trials; ++trial_number) {
    int s = 1 + gsl_rng_uniform_int(sample_threads.thread_info[0].rand_gen,
				    do_iterations);
    // Iterate once to get assignments h_s
    resample(&sample_threads);
    // Copy out the h_s assignments for restoring later
    struct revision_assignment* hs_assignments 
      = copy_revision_assignments(&mmap_info);
    double transition_sum = transition_probability(&sample_threads, max_assignments);
    
    // Sample forward
    for (int it_num = s + 1; it_num <= do_iterations; ++it_num) {
      resample(&sample_threads);
      transition_sum = sum_logs(transition_sum, 
				transition_probability(&sample_threads, 
						       max_assignments));
    }
    // Restore h_s assignments
    resample_restore(&sample_threads, hs_assignments);
    free(hs_assignments);
    // Sample backward
    for (int it_num = s - 1; it_num >= 1; --it_num) {
      resample_reverse(&sample_threads);
      transition_sum = sum_logs(transition_sum, 
				transition_probability(&sample_threads, 
						       max_assignments));
    }
    transition_sum -= log(do_iterations);
    printf("%lf\n", maximized_likelihood - transition_sum);
    fflush(stdout);
    if (trial_number < trials - 1) {
      resample_restore(&sample_threads, max_assignments);
    }
  }

  free(max_assignments);
  destroy_threads(&sample_threads);
  close_mmaps(mmap_info);
}

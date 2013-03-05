/* Initialize topic and POV assignments uniformly at random, and set
   various hyper-parameters needed for inference. Assignments must be
   initialized before doing inference for the first time. */

#include <assert.h>
#include <gsl/gsl_rng.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "index.h"
#include "parse_mmaps.h"
#include "sample.h"

int main(int argc, char **argv) {
  if (argc != 11) {
    printf("Usage: %s mmap_directory num_topics pov_per_topic num_threads psi_alpha "
	   "psi_beta gamma_alpha gamma_beta beta alpha\n",
           argv[0]);
    exit(1);
  }
  int num_threads = atoi(argv[4]);
  char *endptr;
  create_indexes(argv[1],
		 strtod(argv[5], &endptr),
		 strtod(argv[6], &endptr),
		 strtod(argv[7], &endptr),
		 strtod(argv[8], &endptr),
		 strtod(argv[9], &endptr),
		 strtod(argv[10], &endptr),
		 atoi(argv[2]),
		 atoi(argv[3]),
		 num_threads);
  struct mmap_info mmap_info = open_mmaps_mmap(argv[1]);

  // If revisions are numbered from zero, make sure the first
  // revision gets assigned a null topic/POV
  struct revision_assignment* first_assignment
    = get_revision_assignment(&mmap_info, 0);
  first_assignment->topic = -1;
  first_assignment->pov = -1;

  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);
  resample_null(&sample_threads);
  resample_uniform(&sample_threads);

  destroy_threads(&sample_threads);
  close_mmaps(mmap_info);
}

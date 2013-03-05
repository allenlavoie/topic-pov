/* Output the current topic and POV assignments in text in text form
   to stdout, optionally iteratively maximizing the assignments first
   (to find a high-probability assignments). Even if performing
   maximization, the current assignments should be post-burn-in for
   best results. */

#include <assert.h>
#include <gsl/gsl_rng.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"
#include "sample.h"

int main(int argc, char **argv) {
  if (argc != 4) {
    printf("Usage: %s mmap_directory maximization_iterations threads\n",
           argv[0]);
    exit(1);
  }
  int maximization_iterations = atoi(argv[2]);
  int num_threads = atoi(argv[3]);
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  int64_t count_revisions;
  struct revision_assignment* revision_assignments;
  get_revision_assignment_array(&mmap_info, &count_revisions, &revision_assignments);

  if (maximization_iterations > 0) {
    struct sample_threads sample_threads;
    initialize_threads(&sample_threads, num_threads, &mmap_info);
    for (int i = 0; i < maximization_iterations; ++i) {
      resample_maximize(&sample_threads);
    }
    destroy_threads(&sample_threads);
  }
  for (int64_t revision_num = 0; revision_num < count_revisions; ++revision_num) {
    if (revision_assignments[revision_num].pov < 0
	|| revision_assignments[revision_num].topic < 0) {
      continue;
    }
    printf("%" PRId64 " %d %d\n", 
	   revision_num, 
	   revision_assignments[revision_num].topic, 
	   revision_assignments[revision_num].pov);
  }
  close_mmaps(mmap_info);
}

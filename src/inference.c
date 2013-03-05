/* Perform a fixed number of approximate Gibbs sampling iterations,
   splitting the work between multiple threads. Optionally saves every
   Nth sample in the mmaps directory (as
   MMAPS_DIR/saved_assignmentsXXXXX). If compute_likelihood is 0, it
   does not compute the likelihood of the data under the current
   assignments; this can save some time, but makes it difficult to
   determine when the algorithm has converged. 

   Topic and POV assignments must be initialized before inference is
   run for the first time. See initialize.c.*/

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"
#include "sample.h"

int main(int argc, char **argv) {
  if (argc < 4 || argc > 6) {
    printf("Usage: %s mmap_directory iterations threads [save_every_n] [compute_likelihood]\n",
           argv[0]);
    exit(1);
  }
  int save_every_n;
  if (argc >= 5) {
    save_every_n = atoi(argv[4]);
  } else {
    save_every_n = 0;
  }
  int compute_likelihood;
  if (argc >= 6) {
    compute_likelihood = atoi(argv[5]);
  } else {
    compute_likelihood = 1;
  }
  struct mmap_info mmap_info = open_mmaps_memory(argv[1]);
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info.revision_assignment_mmap;
  int do_iterations = atoi(argv[2]);
  int num_threads = atoi(argv[3]);

  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);

  char* saved_revisions_base = full_path(argv[1], "saved_assignments00000");
  char* counter_position = saved_revisions_base + strlen(saved_revisions_base) - 5;
  for (int it_num = 0; it_num < do_iterations; ++it_num) {
    resample(&sample_threads);
    revision_assignment_header->total_iterations++;
    if (save_every_n != 0 && it_num % save_every_n == 0) {
      snprintf(counter_position, 6,
	       "%.5d", it_num / save_every_n);
      write_file(saved_revisions_base, 
		 mmap_info.revision_assignment_mmap, 
		 mmap_info.revision_assignment_mmap_size);
    }
    if (compute_likelihood != 0) {
      printf("%" PRId64 " %lf\n", revision_assignment_header->total_iterations, 
	     parallel_log_likelihood(&sample_threads));
    }
  }
  free(saved_revisions_base);
  destroy_threads(&sample_threads);
  close_mmaps(mmap_info);
}

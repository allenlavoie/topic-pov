/* Verify that indexes properly reflect topic and POV assignments
   after inference by re-initializing, sampling for one iteration,
   "counting down" assignments, and finally verifying that indexes are
   zeroed (and not negative). Does not modify the mmaps. Primarily
   useful for debugging. */

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
  if (argc != 3) {
    printf("Usage: %s mmap_directory num_threads\n",
           argv[0]);
    exit(1);
  }
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info.revision_assignment_mmap;
  int num_threads = atoi(argv[2]);

  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);
  resample(&sample_threads);
  resample_zero(&sample_threads);
  for (int topic_n = 0; topic_n < revision_assignment_header->num_topics; ++topic_n) {
    struct topic_summary* topic_summary;
    struct pov_summary* pov_dist;
    int64_t* page_dist;
    get_topic_summary(&mmap_info, topic_n, &topic_summary, &pov_dist, &page_dist);
    assert(topic_summary->total_revisions == 0);
    assert(topic_summary->revert_general_count == 0);
    assert(topic_summary->norevert_general_count == 0);
    assert(topic_summary->revert_topic_count == 0);
    assert(topic_summary->norevert_topic_count == 0);
    for (int pov_n = 0; pov_n < revision_assignment_header->pov_per_topic; ++pov_n) {
      for (int ant_pov = 0; ant_pov < revision_assignment_header->pov_per_topic; ++ant_pov) {
	if (pov_n == ant_pov) {
	  continue;
	}
	struct pov_summary* pov_summary = get_ant_pov(&mmap_info, pov_dist, pov_n, ant_pov);
	assert(pov_summary->revert_count == 0);
	assert(pov_summary->norevert_count == 0);
      }
    }
  }
  
  destroy_threads(&sample_threads);
  close_mmaps(mmap_info);
}

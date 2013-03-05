/* As an alternative to randomized initialization, this allows
   user-specified topic and POV assignments to be loaded from a text
   file. */

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "index.h"
#include "parse_mmaps.h"

int main(int argc, char **argv) {
  if (argc != 12) {
    printf("Usage: %s mmap_directory num_topics pov_per_topic num_threads "
	   "psi_alpha psi_beta gamma_alpha gamma_beta beta alpha revision_assignments\n",
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

  struct revision_assignment* revision_assignments;
  int64_t count_revisions;
  get_revision_assignment_array(&mmap_info, &count_revisions, &revision_assignments);

  FILE* assignments_file = fopen(argv[11], "r");

  int64_t revision_id;
  int topic;
  int pov;
  while (fscanf(assignments_file, "%" PRId64 " %d %d", &revision_id, &topic, &pov) != EOF) {
    const struct revision* revision = get_revision(&mmap_info, revision_id);
    assert(revision->timestamp != 0 || revision->user != 0 || revision->article != 0);
    revision_assignments[revision_id].topic = topic;
    revision_assignments[revision_id].pov = pov;
    change_indexes(&mmap_info, revision_id, 1);
  }

  fclose(assignments_file);
  close_mmaps(mmap_info);
}

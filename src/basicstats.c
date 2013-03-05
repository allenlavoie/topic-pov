/* Open mmaps and provide basic information about them. Currently this
   is only the number of completed Gibbs sampling iterations. */

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
  if (argc != 2) {
    printf("Usage: %s mmap_directory\n",
           argv[0]);
    exit(1);
  }
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info.revision_assignment_mmap;

  printf("%" PRId64 "\n", revision_assignment_header->total_iterations);
  close_mmaps(mmap_info);
}

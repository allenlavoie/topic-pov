/* Compute a quantity reflecting the antagonism between two users
   (roughly the probability that each would POV-revert the other if
   place next to each other on a page). Useful for querying specific
   pairs of users for debugging, but does not average over multiple
   posterior samples, and is slower for batches of users. See
   page_user_stats.c for computing batch user statistics across
   multiple posterior samples. */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "parse_mmaps.h"
#include "comparisons.h"

int main(int argc, char **argv) {
  if (argc != 4) {
    printf("Usage: %s mmap_directory first_user second_user\n",
           argv[0]);
    exit(1);
  }
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  int first_user = atoi(argv[2]);
  int second_user = atoi(argv[3]);
  printf("%lf\n", user_antagonism(&mmap_info, first_user, second_user));
  close_mmaps(mmap_info);
}

/* Verify that the data stored in the current mmaps matches the
   revisions given in a text file. This only checks the work of
   store_revisions.c; for checking topic and POV assignment indexes,
   see check_indexes.c. */

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "index.h"
#include "parse_mmaps.h"

void verify_mmaps(const char* file, struct mmap_info mmap_info) {
  FILE* revision_in = fopen(file, "r");
  int32_t page_id;
  int32_t user_id;
  int64_t revision_id;
  int64_t timestamp;
  int64_t parent;
  char disagrees;

  int32_t previous_user = -1;
  int32_t previous_page = -1;
  int64_t count_user_revisions;
  int64_t count_page_revisions;
  const int64_t* user_revisions;
  const int64_t* current_user_revision = NULL;
  const int64_t* page_revisions;
  const int64_t* current_page_revision = NULL;
  while (fscanf(revision_in, "%d\t%" PRId64 "\t%d\t%" PRId64 "\t%" PRId64 "\t%c",
		&page_id, &timestamp, &user_id, &revision_id, &parent, &disagrees) != EOF) {
    const struct revision* revision_mmap = get_revision(&mmap_info, revision_id);
    assert(revision_mmap->article == page_id);
    assert(revision_mmap->timestamp == timestamp);
    assert(revision_mmap->user == user_id);
    if (revision_mmap->parent == -1 && parent >= 0) {
      // Could have multiple revisions reverting a single parent, 
      // in which case it's OK that this revision was nulled out.
      // The parent could also be on a different page.
      assert((get_revision(&mmap_info, parent)->child >= 0
              && get_revision(&mmap_info, parent)->child != revision_id)
             || get_revision(&mmap_info, parent)->article != page_id);
    } else {
      assert(revision_mmap->parent == parent);
    }
    if (revision_mmap->child >= 0) {
      assert(get_revision(&mmap_info, revision_mmap->child)->parent == revision_id);
    }
    if (revision_mmap->parent >= 0) {
      assert(get_revision(&mmap_info, revision_mmap->parent)->child == revision_id);
    }
    if (previous_user != user_id) {
      get_user(&mmap_info, user_id, &count_user_revisions, &user_revisions);
      previous_user = user_id;
      current_user_revision = user_revisions;
    }
    while (*current_user_revision != revision_id) {
      assert(current_user_revision - user_revisions < count_user_revisions);
      current_user_revision++;
    }
    if (previous_page != page_id) {
      get_page(&mmap_info, page_id, &count_page_revisions, &page_revisions);
      previous_page = page_id;
      current_page_revision = page_revisions;
    }
    assert(current_page_revision - page_revisions < count_page_revisions);
    assert(*current_page_revision == revision_id);
    current_page_revision++;
  }
  fclose(revision_in);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    printf("Usage: %s mmap_directory revision_input_file\n",
           argv[0]);
    exit(1);
  }
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  verify_mmaps(argv[1], mmap_info);
  close_mmaps(mmap_info);
}

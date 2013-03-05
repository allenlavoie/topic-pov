/* Takes input data in the following format:
   page_id timestamp user_id revision_id parent_revision_id disagrees_with_parent

   All fields are integers, except disagrees_with_parent, which must
   be 't' or 'f'. Creates page_index_mmap, revisions_mmap, and
   user_index_mmap using this information. Currently the timestamp
   field is not used. Topic and POV assignments must then be
   initialized before inference can take place. */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "index.h"
#include "parse_mmaps.h"

struct revision_stats {
  int64_t total_revisions;
  int64_t max_revision_id;
  int32_t max_user_id;
  int32_t max_page_id;
};

void get_file_stats(const char* file, struct revision_stats* stats) {
  stats->total_revisions = 0;
  stats->max_revision_id = -1;
  stats->max_user_id = -1;
  stats->max_page_id = -1;
  FILE* revision_in = fopen(file, "r");
  int32_t page_id;
  int32_t user_id;
  int64_t revision_id;
  int64_t timestamp;
  int64_t parent;
  char disagrees;
  while (fscanf(revision_in, "%d\t%" PRId64 "\t%d\t%" PRId64 "\t%" PRId64 "\t%c",
		&page_id, &timestamp, &user_id, &revision_id, &parent, &disagrees) != EOF) {
    if (user_id > stats->max_user_id) {
      stats->max_user_id = user_id;
    }
    if (page_id > stats->max_page_id) {
      stats->max_page_id = page_id;
    }
    if (revision_id > stats->max_revision_id) {
      stats->max_revision_id = revision_id;
    }
    stats->total_revisions += 1;
  }
  fclose(revision_in);
}

int64_t user_index_mmap_size(const struct revision_stats* stats) {
  return sizeof(struct user_header)
    + sizeof(struct user_index) * (stats->max_user_id + 1)
    + sizeof(int64_t) * stats->total_revisions;
}

int64_t page_index_mmap_size(const struct revision_stats* stats) {
  return sizeof(struct page_header)
    + sizeof(struct page_index) * (stats->max_page_id + 1)
    + sizeof(int64_t) * stats->total_revisions;
}

int64_t revision_mmap_size(const struct revision_stats* stats) {
  return sizeof(struct revision_header)
    + sizeof(struct revision) * (stats->max_revision_id + 1);
}

void fill_mmaps(const char* file,
		const struct revision_stats* stats,
		char* user_index_mmap,
		char* page_index_mmap,
		char* revisions_mmap) {
  struct revision_header* revision_header = (struct revision_header*)revisions_mmap;
  struct user_index* user_array = (struct user_index*)(user_index_mmap + sizeof(struct user_header));
  struct page_index* page_array = (struct page_index*)(page_index_mmap + sizeof(struct page_header));
  struct revision* revision_array = (struct revision*)(revisions_mmap + sizeof(struct revision_header));
  FILE* revision_in = fopen(file, "r");
  int32_t page_id;
  int32_t user_id;
  int64_t revision_id;
  int64_t timestamp;
  int64_t parent;
  char disagrees;
  for (int64_t i = 0; i < revision_header->count_revisions; ++i) {
    revision_array[i].child = -1;
  }
  while (fscanf(revision_in, "%d\t%" PRId64 "\t%d\t%" PRId64 "\t%" PRId64 "\t%c",
		&page_id, &timestamp, &user_id, &revision_id, &parent, &disagrees) != EOF) {
    revision_array[revision_id].article = page_id;
    revision_array[revision_id].user = user_id;
    revision_array[revision_id].parent = parent;
    if (parent >= 0) {
      if (revision_array[parent].article != page_id) {
        // This revision's parent is on a different page.
        // Ignore cross-page relationships for now.
        revision_array[revision_id].parent = -1;
      } else if (revision_array[parent].child != -1) {
	// Rarely, two revisions will both revert the same parent.
	// We only count the first as reverting that parent revision.
	revision_array[revision_id].parent = -1;
      } else {
	revision_array[parent].child = revision_id;
      }
    }
    revision_array[revision_id].timestamp = timestamp;
    revision_array[revision_id].disagrees = (disagrees == 't');

    page_array[page_id].count_revisions++;
    user_array[user_id].count_revisions++;
  }
  fclose(revision_in);
  revision_in = fopen(file, "r");
  int64_t current_user_offset =
    sizeof(struct user_header)
    + sizeof(struct user_index) * (stats->max_user_id + 1);
  int64_t current_page_offset =
    sizeof(struct page_header)
    + sizeof(struct page_index) * (stats->max_page_id + 1);
  while (fscanf(revision_in, "%d\t%" PRId64 "\t%d\t%" PRId64 "\t%" PRId64 "\t%c",
		&page_id, &timestamp, &user_id, &revision_id, &parent, &disagrees) != EOF) {
    if (page_array[page_id].revisions_offset == 0) {
      page_array[page_id].revisions_offset = current_page_offset;
      current_page_offset += sizeof(int64_t) * page_array[page_id].count_revisions;
      page_array[page_id].count_revisions = 0;
    }
    *((int64_t*)(page_index_mmap + page_array[page_id].revisions_offset)
      + page_array[page_id].count_revisions) = revision_id;
    page_array[page_id].count_revisions++;

    if (user_array[user_id].revisions_offset == 0) {
      user_array[user_id].revisions_offset = current_user_offset;
      current_user_offset += sizeof(int64_t) * user_array[user_id].count_revisions;
      user_array[user_id].count_revisions = 0;
    }
    *((int64_t*)(user_index_mmap + user_array[user_id].revisions_offset)
      + user_array[user_id].count_revisions) = revision_id;
    user_array[user_id].count_revisions++;
  }
  fclose(revision_in);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    printf("Usage: %s mmap_directory revision_input_file\n",
           argv[0]);
    exit(1);
  }
  struct revision_stats stats;
  get_file_stats(argv[2], &stats);
  printf("%d max user, %d max page, %"PRId64" max revision, %"PRId64" total revisions\n",
	 stats.max_user_id, stats.max_page_id, stats.max_revision_id, stats.total_revisions);
  char *user_index_mmap_name = full_path(argv[1], USER_INDEX_MMAP_NAME);
  char *page_index_mmap_name = full_path(argv[1], PAGE_INDEX_MMAP_NAME);
  char *revisions_mmap_name = full_path(argv[1], REVISIONS_MMAP_NAME);
  char *user_index_mmap = create_mmap(user_index_mmap_name, user_index_mmap_size(&stats));
  char *page_index_mmap = create_mmap(page_index_mmap_name, page_index_mmap_size(&stats));
  char *revisions_mmap = create_mmap(revisions_mmap_name, revision_mmap_size(&stats));
  ((struct user_header*)user_index_mmap)->count_users = stats.max_user_id + 1;
  ((struct page_header*)page_index_mmap)->count_pages = stats.max_page_id + 1;
  ((struct revision_header*)revisions_mmap)->count_revisions = stats.max_revision_id + 1;
  fill_mmaps(argv[2], &stats, user_index_mmap, page_index_mmap, revisions_mmap);
  free(user_index_mmap_name);
  free(page_index_mmap_name);
  free(revisions_mmap_name);
  return 0;
}

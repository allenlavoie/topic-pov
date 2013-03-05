#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "index.h"
#include "parse_mmaps.h"
#include "sample.h"

const char* USER_INDEX_MMAP_NAME = "user_index_mmap";
const char* PAGE_INDEX_MMAP_NAME = "page_index_mmap";
const char* REVISIONS_MMAP_NAME = "revisions_mmap";
const char* REVISION_ASSIGNMENT_MMAP_NAME = "revision_assignment_mmap";
const char* TOPIC_INDEX_MMAP_NAME = "topic_index_mmap";
const char* USER_TOPIC_MMAP_NAME = "user_topic_mmap";

struct mmap_info open_mmaps_internal(const char* directory,
				     int rw_mmaps_inmem,
				     int read_only,
				     int exclude_inference);

char* full_path(const char* directory, const char* file) {
  char* ret = malloc(strlen(directory) + 1 + strlen(file) + 1);
  strcpy(ret, directory);
  if (directory[strlen(directory) - 1] != '/') {
    strcat(ret, "/");
  }
  strcat(ret, file);
  return ret;
}

struct mmap_info open_mmaps_readonly(const char* directory) {
  return open_mmaps_internal(directory, 0, 1, 0);
}

struct mmap_info open_mmaps_memory(const char* directory) {
  return open_mmaps_internal(directory, 1, 0, 0);
}

struct mmap_info open_mmaps_mmap(const char* directory) {
  return open_mmaps_internal(directory, 0, 0, 0);
}

char *create_mmap(const char *file_name, int64_t length) {
  int outfd = open(file_name,
		   O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);
  if (outfd < 0) {
    fprintf(stderr, "Could not create mmap file\n");
    exit(1);
  }
  lseek(outfd, length - 1, SEEK_SET);
  if (write(outfd, "", 1) != 1) {
    fprintf(stderr, "Could not write to file\n");
    exit(1);
  }
  lseek(outfd, 0, SEEK_SET);
  char *mmap_addr = mmap(NULL, length,
                         PROT_READ | PROT_WRITE, MAP_SHARED, outfd, 0);
  if (mmap_addr == MAP_FAILED) {
    fprintf(stderr, "Could not memory map file\n");
    exit(1);
  }
  // memset(mmap_addr, 0, length); (it's actually zeroed by default)
  close(outfd);
  return mmap_addr;
}

char* open_mmap(const char *file_name, int can_write, int64_t* mmap_size) {
  int mmapfd;
  if (file_name == NULL) {
    mmapfd = -1;
    return NULL;
  }
  if (can_write) {
    mmapfd = open(file_name, O_RDWR);
  } else {
    mmapfd = open(file_name, O_RDONLY);
  }
  if (mmapfd < 0) {
    return NULL;
  }
  struct stat statbuf;
  if (fstat(mmapfd, &statbuf) == -1) {
    fprintf(stderr, "Could not stat file %s\n", file_name);
    exit(1);
  }
  lseek(mmapfd, 0, SEEK_SET);
  char *mmap_addr;
  if (can_write) {
    mmap_addr = mmap(NULL, statbuf.st_size,
		     PROT_READ | PROT_WRITE, MAP_SHARED, mmapfd, 0);
  } else {
    // Create a private mapping that is copy-on-write. 
    // We can write to the mapping, but the changes are not
    // committed back to the file (which we open as read-only).
    mmap_addr = mmap(NULL, statbuf.st_size,
		     PROT_READ | PROT_WRITE, MAP_PRIVATE, mmapfd, 0);
  }
  if (mmap_addr == MAP_FAILED) {
    fprintf(stderr, "Could not memory map file %s\n",
            file_name);
    exit(1);
  }
  *mmap_size = statbuf.st_size;
  close(mmapfd);
  return mmap_addr;
}

char* open_mmap_read(const char* file_name, int64_t* mmap_size) {
  return open_mmap(file_name, 0, mmap_size);
}

char* open_mmap_rw(const char* file_name, int64_t* mmap_size) {
  return open_mmap(file_name, 1, mmap_size);
}

char* read_file(const char* file_name, int64_t* length) {
  int fd = open(file_name, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "Could not open mmap file %s\n",
            file_name);
    exit(1);
  }
  struct stat statbuf;
  if (fstat(fd, &statbuf) == -1) {
    fprintf(stderr, "Could not stat file %s\n", file_name);
    exit(1);
  }
  *length = statbuf.st_size;
  char* ret = malloc(sizeof(char) * statbuf.st_size);
  FILE* fobj = fdopen(fd, "r");
  assert(fread(ret, sizeof(char), statbuf.st_size, fobj) == statbuf.st_size);
  fclose(fobj);
  return ret;
}

void write_file(const char* file_name, const char* to_write, int64_t length) {
  FILE* fobj = fopen(file_name, "w+");
  if (fobj == NULL) {
    fprintf(stderr, "Could not open file %s\n", file_name);
  }
  assert(fwrite(to_write, sizeof(char), length, fobj) == length);
  fclose(fobj);
}

struct mmap_info open_mmaps_internal(const char* directory,
				     int rw_mmaps_inmem,
				     int read_only,
				     int exclude_inference) {
  struct mmap_info ret;
  ret.revisions_mmap_name = full_path(directory, REVISIONS_MMAP_NAME);
  ret.user_mmap_name = full_path(directory, USER_INDEX_MMAP_NAME);
  ret.page_mmap_name = full_path(directory, PAGE_INDEX_MMAP_NAME);
  ret.revision_assignment_mmap_name = full_path(directory, REVISION_ASSIGNMENT_MMAP_NAME);
  ret.topic_index_mmap_name = full_path(directory, TOPIC_INDEX_MMAP_NAME);
  ret.user_topic_mmap_name = full_path(directory, USER_TOPIC_MMAP_NAME);
  ret.rw_mmaps_inmem = rw_mmaps_inmem;
  ret.revision_mmap = open_mmap_read(ret.revisions_mmap_name, &(ret.revision_mmap_size));
  ret.user_mmap = open_mmap_read(ret.user_mmap_name, &(ret.user_mmap_size));
  ret.page_mmap = open_mmap_read(ret.page_mmap_name, &(ret.page_mmap_size));

  if (exclude_inference) {
    ret.revision_assignment_mmap = NULL;
    ret.topic_index_mmap = NULL;
    ret.user_topic_mmap = NULL;
  } else {
    if (read_only) {
      ret.rw_mmaps_inmem = 0;
      ret.revision_assignment_mmap = open_mmap_read(ret.revision_assignment_mmap_name, 
						    &(ret.revision_assignment_mmap_size));
      ret.topic_index_mmap = open_mmap_read(ret.topic_index_mmap_name,
					    &(ret.topic_index_mmap_size));
      ret.user_topic_mmap = open_mmap_read(ret.user_topic_mmap_name,
					   &(ret.user_topic_mmap_size));
    } else {
      if (rw_mmaps_inmem) {
	ret.revision_assignment_mmap = read_file(ret.revision_assignment_mmap_name,
						 &(ret.revision_assignment_mmap_size));
	ret.topic_index_mmap = read_file(ret.topic_index_mmap_name,
					 &(ret.topic_index_mmap_size));
	ret.user_topic_mmap = read_file(ret.user_topic_mmap_name,
					&(ret.user_topic_mmap_size));
      } else {
	ret.revision_assignment_mmap = open_mmap_rw(ret.revision_assignment_mmap_name, 
						    &(ret.revision_assignment_mmap_size));
	ret.topic_index_mmap = open_mmap_rw(ret.topic_index_mmap_name,
					    &(ret.topic_index_mmap_size));
	ret.user_topic_mmap = open_mmap_rw(ret.user_topic_mmap_name,
					   &(ret.user_topic_mmap_size));
      }
    }
  }
  if (ret.topic_index_mmap != NULL) {
    struct topic_summary_header* topic_summary_header
      = (struct topic_summary_header*)(ret.topic_index_mmap);
    int64_t pov_size = sizeof(struct pov_summary) * topic_summary_header->pov_per_topic 
      * (topic_summary_header->pov_per_topic - 1);
    ret.topic_index_pages_mmap = ret.topic_index_mmap + sizeof(struct topic_summary_header) 
      + (pov_size + sizeof(struct topic_summary)) * topic_summary_header->num_topics;
  }
  
  
  return ret;
}

void close_mmaps(struct mmap_info mmap_info) {
  if (mmap_info.revision_mmap != NULL) {
    assert(munmap((void*)(mmap_info.revision_mmap), mmap_info.revision_mmap_size)
	   == 0);
  }
  if (mmap_info.user_mmap != NULL) {
    assert(munmap((void*)(mmap_info.user_mmap), mmap_info.user_mmap_size)
	   == 0);
  }
  if (mmap_info.page_mmap != NULL) {
    assert(munmap((void*)(mmap_info.page_mmap), mmap_info.page_mmap_size)
	   == 0);
  }
  if (mmap_info.rw_mmaps_inmem) {
    write_file(mmap_info.revision_assignment_mmap_name, 
	       mmap_info.revision_assignment_mmap,
	       mmap_info.revision_assignment_mmap_size);
    write_file(mmap_info.topic_index_mmap_name,
	       mmap_info.topic_index_mmap,
	       mmap_info.topic_index_mmap_size);
    write_file(mmap_info.user_topic_mmap_name,
	       mmap_info.user_topic_mmap,
	       mmap_info.user_topic_mmap_size);
    free(mmap_info.revision_assignment_mmap);
    free(mmap_info.topic_index_mmap);
    free(mmap_info.user_topic_mmap);
  } else {
    if (mmap_info.revision_assignment_mmap != NULL) {
      assert(munmap(mmap_info.revision_assignment_mmap, 
		    mmap_info.revision_assignment_mmap_size)
	     == 0);
    }
    if (mmap_info.topic_index_mmap != NULL) {
      assert(munmap(mmap_info.topic_index_mmap, mmap_info.topic_index_mmap_size)
	     == 0);
    }
    if (mmap_info.user_topic_mmap != NULL) {
      assert(munmap(mmap_info.user_topic_mmap, mmap_info.user_topic_mmap_size)
	     == 0);
    }
  }
  free(mmap_info.revisions_mmap_name);
  free(mmap_info.user_mmap_name);
  free(mmap_info.page_mmap_name);
  free(mmap_info.revision_assignment_mmap_name);
  free(mmap_info.topic_index_mmap_name);
  free(mmap_info.user_topic_mmap_name);
}

const struct revision* get_revision(const struct mmap_info* mmap_info, int64_t revision_id) {
  assert(mmap_info->revision_mmap != NULL);
  return (const struct revision*)(mmap_info->revision_mmap + sizeof(struct revision_header)) + revision_id;
}

void get_page(const struct mmap_info* mmap_info, int64_t page_id, int64_t* count_revisions, 
	      const int64_t** revision_ids) {
  assert(mmap_info->page_mmap != NULL);
  struct page_index* page_index = (struct page_index*)(mmap_info->page_mmap
						       + sizeof(struct page_header)) + page_id;
  *count_revisions = page_index->count_revisions;
  *revision_ids = (const int64_t*)(mmap_info->page_mmap + page_index->revisions_offset);
}

void get_user(const struct mmap_info* mmap_info, int64_t user_id, int64_t* count_revisions,
	      const int64_t** revision_ids) {
  assert(mmap_info->user_mmap != NULL);
  struct user_index* user_index = (struct user_index*)(mmap_info->user_mmap
						       + sizeof(struct user_header)) + user_id;
  *count_revisions = user_index->count_revisions;
  if (revision_ids != NULL) {
    *revision_ids = (const int64_t*)(mmap_info->user_mmap + user_index->revisions_offset);
  }
}

void initialize_user_topics(const struct mmap_info* mmap_info, int sample, int modn) {
  const struct user_topic_header* user_topic_header
    = (const struct user_topic_header*)mmap_info->user_topic_mmap;
  const struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  for (int64_t user_id = sample; user_id < user_topic_header->num_users; user_id += modn) {
    double* topic_pov_dist = (double*)(mmap_info->user_topic_mmap + sizeof(struct user_topic_header))
      + user_id * user_topic_header->num_topics * user_topic_header->pov_per_topic;
    for (int topic = 0; topic < user_topic_header->num_topics; ++topic) {
      for (int pov = 0; pov < user_topic_header->pov_per_topic; ++pov) {
	topic_pov_dist[topic * user_topic_header->pov_per_topic + pov] = revision_assignment_header->alpha;
      }
    }
  }
}

void get_user_topics(const struct mmap_info* mmap_info, int64_t user_id, double** topic_pov_dist) {
  const struct user_topic_header* user_topic_header
    = (const struct user_topic_header*)mmap_info->user_topic_mmap;
  *topic_pov_dist = (double*)(mmap_info->user_topic_mmap 
			      + sizeof(struct user_topic_header))
    + user_id * user_topic_header->num_topics * user_topic_header->pov_per_topic;
}

void get_topic_summary(const struct mmap_info* mmap_info, int32_t topic_id,
		       struct topic_summary** topic_summary,
		       struct pov_summary** pov_dist,
		       int64_t** page_dist){
  assert(mmap_info->topic_index_mmap != NULL);
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)(mmap_info->topic_index_mmap);
  int64_t pov_size = sizeof(struct pov_summary) * topic_summary_header->pov_per_topic 
    * (topic_summary_header->pov_per_topic - 1);
  char* base = mmap_info->topic_index_mmap + sizeof(struct topic_summary_header)
    + topic_id * (pov_size + sizeof(struct topic_summary));
  if (topic_summary != NULL) {
    *topic_summary = (struct topic_summary*)base;
  }
  if (pov_dist != NULL) {
    *pov_dist = (struct pov_summary*)(base + sizeof(struct topic_summary));
  }
  if (page_dist != NULL) {
    assert(mmap_info->topic_index_pages_mmap != NULL);
    int64_t page_size = sizeof(int64_t) * topic_summary_header->num_pages;
    *page_dist = (int64_t*)(mmap_info->topic_index_pages_mmap + page_size * topic_id);
  }
}

void get_revision_assignment_array(const struct mmap_info* mmap_info, int64_t* count_revisions, 
				   struct revision_assignment** revisions) {
  assert(mmap_info->revision_assignment_mmap != NULL);
  *count_revisions = ((struct revision_assignment_header*)mmap_info->revision_assignment_mmap)->count_revisions;
  *revisions = (struct revision_assignment*)(mmap_info->revision_assignment_mmap 
					     + sizeof(struct revision_assignment_header));
}

struct revision_assignment* get_revision_assignment(const struct mmap_info* mmap_info, int64_t revision_id) {
  assert(mmap_info->revision_assignment_mmap != NULL);
  return (struct revision_assignment*)(mmap_info->revision_assignment_mmap 
				       + sizeof(struct revision_assignment_header)) + revision_id;
}

struct pov_summary* get_ant_pov(const struct mmap_info* mmap_info, 
				struct pov_summary* pov_array, int32_t pov, int32_t ant_pov) {
  assert(ant_pov != pov);
  const struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info->topic_index_mmap;
  if (ant_pov > pov) {
    ant_pov -= 1;
  }
  return pov_array + (topic_summary_header->pov_per_topic - 1) * pov + ant_pov;
}

void change_indexes(const struct mmap_info* mmap_info, int64_t revision_id, int32_t change_by) {
  struct topic_summary* topic_summary;
  struct pov_summary* pov_dist;
  int64_t* page_dist;
  const struct revision_assignment* revision_assignment
    = get_revision_assignment(mmap_info, revision_id);
  const struct revision* revision = get_revision(mmap_info, revision_id);
  assert(revision_assignment->pov >= 0);
  assert(revision_assignment->topic >= 0);

  get_topic_summary(mmap_info, revision_assignment->topic, 
		    &topic_summary, &pov_dist, &page_dist);

  assert((page_dist[revision->article] += change_by) >= 0);
  assert((topic_summary->total_revisions += change_by) >= 0);

  double* user_dist;
  const struct user_topic_header* user_topic_header
    = (const struct user_topic_header*)(mmap_info->user_topic_mmap);
  get_user_topics(mmap_info, revision->user, &user_dist);
  assert((user_dist[user_topic_header->pov_per_topic * revision_assignment->topic
		    + revision_assignment->pov] += change_by) >= 0);
  if (revision->parent >= 0) {
    assert(revision->parent < revision_id);
    const struct revision_assignment* parent_revision_assignment
      = get_revision_assignment(mmap_info, revision->parent);
    if (parent_revision_assignment->topic == revision_assignment->topic) {
      if (parent_revision_assignment->pov != revision_assignment->pov) {
	struct pov_summary* pov_summary 
	  = get_ant_pov(mmap_info, pov_dist, 
			revision_assignment->pov,
			parent_revision_assignment->pov);
	if (revision->disagrees) {
	  assert((pov_summary->revert_count += change_by) >= 0);
	} else {
	  assert((pov_summary->norevert_count += change_by) >= 0);
	}
      } else {
	if (revision->disagrees) {
	  assert((topic_summary->revert_topic_count += change_by) >= 0);
	} else {
	  assert((topic_summary->norevert_topic_count += change_by) >= 0);
	}
      }
    } else {
      if (revision->disagrees) {
	assert((topic_summary->revert_general_count += change_by) >= 0);
      } else {
	assert((topic_summary->norevert_general_count += change_by) >= 0);
      }
    }
  }
}

void change_revision_assignment(const struct mmap_info* mmap_info, int64_t revision,
				int32_t new_topic, int32_t new_pov) {
  int64_t count_revisions;
  struct revision_assignment* revision_assignments;
  get_revision_assignment_array(mmap_info, &count_revisions, &revision_assignments);
  assert(revision_assignments[revision].topic >= 0);
  assert(revision_assignments[revision].pov >= 0);
  const struct revision* revision_info = get_revision(mmap_info, revision);
  if (revision_info->child >= 0) {
    // We could also be changing the type of revert for an edit reverting this one
    change_indexes(mmap_info, revision_info->child, -1);
  }
  change_indexes(mmap_info, revision, -1);
  revision_assignments[revision].topic = new_topic;
  revision_assignments[revision].pov = new_pov;
  change_indexes(mmap_info, revision, 1);
  if (revision_info->child >= 0) {
    change_indexes(mmap_info, revision_info->child, 1);
  }
}

int64_t revision_assignment_mmap_size(int64_t count_revisions) {
  return sizeof(struct revision_assignment_header) + sizeof(struct revision_assignment) * count_revisions;
}

int64_t topic_summary_mmap_size(int32_t num_topics, int32_t pov_per_topic, int64_t num_pages) {
  return sizeof(struct topic_summary_header) + sizeof(struct topic_summary) * num_topics
    + (int64_t)num_topics * sizeof(int64_t) * num_pages
    + (int64_t)num_topics * sizeof(struct pov_summary) * pov_per_topic * (pov_per_topic - 1);
}

int64_t user_topic_mmap_size(int64_t num_users, int32_t num_topics, int32_t pov_per_topic) {
  return sizeof(struct user_topic_header) + num_users * sizeof(double) * num_topics * pov_per_topic;
}

void create_indexes(const char* directory,
		    double psi_alpha,
		    double psi_beta,
		    double gamma_alpha,
		    double gamma_beta,
		    double beta,
		    double alpha,
		    int num_topics,
		    int pov_per_topic,
		    int num_threads) {
  struct mmap_info mmap_info = open_mmaps_internal(directory, 0, 1, 1);
  mmap_info.topic_index_mmap_size
    = topic_summary_mmap_size(num_topics, pov_per_topic,
			      ((struct page_header*)(mmap_info.page_mmap))
			      ->count_pages);
  mmap_info.topic_index_mmap
    = create_mmap(mmap_info.topic_index_mmap_name, 
		  mmap_info.topic_index_mmap_size);
  mmap_info.revision_assignment_mmap_size 
    = revision_assignment_mmap_size(((struct revision_header*)
				     (mmap_info.revision_mmap))->count_revisions);
  mmap_info.revision_assignment_mmap 
    = create_mmap(mmap_info.revision_assignment_mmap_name, 
		  mmap_info.revision_assignment_mmap_size);
  mmap_info.user_topic_mmap_size 
    = user_topic_mmap_size(((struct user_header*)(mmap_info.user_mmap))
			   ->count_users, num_topics, pov_per_topic);
  mmap_info.user_topic_mmap 
    = create_mmap(mmap_info.user_topic_mmap_name, 
		  mmap_info.user_topic_mmap_size);
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info.revision_assignment_mmap;
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info.topic_index_mmap;
  struct user_topic_header* user_topic_header
    = (struct user_topic_header*)mmap_info.user_topic_mmap;
  revision_assignment_header->psi_alpha = psi_alpha;
  revision_assignment_header->psi_beta = psi_beta;
  revision_assignment_header->gamma_alpha = gamma_alpha;
  revision_assignment_header->gamma_beta = gamma_beta;
  revision_assignment_header->beta = beta;
  revision_assignment_header->alpha = alpha;
  revision_assignment_header->total_iterations = 0;
  revision_assignment_header->count_revisions
    = ((struct revision_header*)(mmap_info.revision_mmap))->count_revisions;
  revision_assignment_header->num_topics = num_topics;
  revision_assignment_header->pov_per_topic = pov_per_topic;

  topic_summary_header->num_topics = num_topics;
  topic_summary_header->pov_per_topic = pov_per_topic;
  topic_summary_header->num_pages
    = ((struct page_header*)(mmap_info.page_mmap))->count_pages;

  user_topic_header->num_topics = num_topics;
  user_topic_header->pov_per_topic = pov_per_topic;
  user_topic_header->num_users
    = ((struct user_header*)(mmap_info.user_mmap))->count_users;

  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);
  parallel_initialize_user_topics(&sample_threads);
  destroy_threads(&sample_threads);

  close_mmaps(mmap_info);
}

struct revision_assignment* copy_revision_assignments(const struct mmap_info* mmap_info) {
  int64_t count_revisions;
  struct revision_assignment* original_array;
  get_revision_assignment_array(mmap_info, &count_revisions, &original_array);
  int64_t byte_size = sizeof(struct revision_assignment) * count_revisions;
  struct revision_assignment* ret = (struct revision_assignment*)malloc(byte_size);
  memcpy(ret, original_array, byte_size);
  return ret;
}

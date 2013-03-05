/* Compute various statistics about pages, users, and pairs of users
   across one or more posterior samples. These are saved to
   pages_stats.txt, users_stats.txt, and user_comparisons.txt in the
   current directory. Typically the assignments (posterior samples)
   will come those saved using inference.c. Does not modify the
   current mmaps.*/

#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "comparisons.h"
#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"
#include "sample.h"

#define NON_VAR_ARGS 4

struct user_pair_stats {
  int64_t first_user;
  int64_t second_user;
  double antagonism;
};

struct page_user_stats {
  double pov_revert_revert_fraction;
  double pov_revert_edit_fraction;
  double pov_reverted_edit_fraction;
  double pov_reverts;
  double edits;
  double edit_fraction_on_max_pov;
  double max_pov_antagonism;
  double max_pov_antagonism_edit_fraction;
  double max_topic_antagonism;
  double max_topic_antagonism_pov_edit_fraction;
  double max_topic_rv_general;
  double max_topic_rv_topic;
  double edits_on_max_pov;
  double entropy;
};

struct thread_args {
  struct mmap_info* mmap_info;
  struct pov_workspace* pov_workspace;
  double* controversy_by_pov;
  void* data;
  int64_t max_val;
  int thread_num;
  int num_threads;
  void (*worker)(struct mmap_info* mmap_info, 
		 struct pov_workspace* pov_workspace, 
		 double* controversy_by_pov, 
		 void* data, int64_t index);
};

void update_stats(const struct mmap_info* mmap_info, 
		  struct pov_workspace* pov_workspace,
		  const double* controversy_by_pov,
		  void (*get_revisions)(const struct mmap_info*, int64_t, int64_t*, const int64_t**),
		  int64_t user_page_id,
		  struct page_user_stats* stats);

void* do_thread_work(void* args) {
  struct thread_args* thread_args = args;
  for (int64_t i = thread_args->thread_num; 
       i < thread_args->max_val;
       i += thread_args->num_threads) {
    thread_args->worker(thread_args->mmap_info,
			thread_args->pov_workspace,
			thread_args->controversy_by_pov,
			thread_args->data, i);
  }
  return NULL;
}

void parallel_work(struct mmap_info* mmap_info, 
		   struct pov_workspace* pov_workspaces, 
		   double* controversy_by_pov, 
		   void* data, int64_t max_val,
		   int num_threads,
		   void (*worker)(struct mmap_info* mmap_info, 
				  struct pov_workspace* pov_workspace, 
				  double* controversy_by_pov, 
				  void* data, int64_t index)) {
  pthread_t* threads = malloc(sizeof(pthread_t) * num_threads);
  struct thread_args* thread_args = calloc(num_threads, sizeof(struct thread_args));
  for (int thread_num = 0; thread_num < num_threads; ++thread_num) {
    thread_args[thread_num].mmap_info = mmap_info;
    thread_args[thread_num].pov_workspace = pov_workspaces + thread_num;
    thread_args[thread_num].controversy_by_pov = controversy_by_pov;
    thread_args[thread_num].data = data;
    thread_args[thread_num].max_val = max_val;
    thread_args[thread_num].thread_num = thread_num;
    thread_args[thread_num].num_threads = num_threads;
    thread_args[thread_num].worker = worker;
    pthread_create(threads + thread_num, NULL, 
		   do_thread_work, (void*)(thread_args + thread_num));
  }
  void* res;
  for (int thread_num = 0; thread_num < num_threads; ++thread_num) {
    pthread_join(threads[thread_num], &res);
  }
  free(threads);
  free(thread_args);
}

void update_user_antagonism(struct mmap_info* mmap_info, 
			    struct pov_workspace* pov_workspace, 
			    double* controversy_by_pov, 
			    void* data, int64_t index) {
  struct user_pair_stats* user_pair_stats = data;
  user_pair_stats[index].antagonism
    += user_antagonism(mmap_info, user_pair_stats[index].first_user,
		       user_pair_stats[index].second_user);
}

void update_user_stats(struct mmap_info* mmap_info, 
		       struct pov_workspace* pov_workspace, 
		       double* controversy_by_pov, 
		       void* data, int64_t index) {
  struct page_user_stats* page_user_stats = data;
  update_stats(mmap_info, pov_workspace, 
	       controversy_by_pov, get_user, 
	       index, page_user_stats + index);
}

void update_page_stats(struct mmap_info* mmap_info, 
		       struct pov_workspace* pov_workspace, 
		       double* controversy_by_pov, 
		       void* data, int64_t index) {
  struct page_user_stats* page_user_stats = data;
  update_stats(mmap_info, pov_workspace, 
	       controversy_by_pov, get_page, 
	       index, page_user_stats + index);
}

void print_stats(struct page_user_stats* stats, int64_t id, int samples, FILE* out) {
  fprintf(out, "%"PRId64" %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf\n",
	  id,
	  stats->pov_revert_revert_fraction / samples,
	  stats->pov_revert_edit_fraction / samples,
	  stats->pov_reverted_edit_fraction / samples,
	  stats->pov_reverts / samples,
	  stats->edits / samples,
	  stats->edit_fraction_on_max_pov / samples,
	  stats->max_pov_antagonism / samples,
	  stats->max_pov_antagonism_edit_fraction / samples,
	  stats->max_topic_antagonism / samples,
	  stats->max_topic_antagonism_pov_edit_fraction / samples,
	  stats->max_topic_rv_general / samples,
	  stats->max_topic_rv_topic / samples,
	  stats->edits_on_max_pov / samples,
	  stats->entropy / samples);
}

void update_stats(const struct mmap_info* mmap_info, 
		  struct pov_workspace* pov_workspace,
		  const double* controversy_by_pov,
		  void (*get_revisions)(const struct mmap_info*, int64_t, int64_t*, const int64_t**),
		  int64_t user_page_id,
		  struct page_user_stats* stats) {
  int64_t count_revisions;
  const int64_t* revision_ids;
  get_revisions(mmap_info, user_page_id, &count_revisions, &revision_ids);
  if (count_revisions == 0) {
    return;
  }
  int64_t count_on_max;
  int max_topic;
  int max_pov;
  double entropy;
  edits_on_max_pov(mmap_info, pov_workspace, revision_ids, count_revisions,
		   &count_on_max, &max_topic, &max_pov, &entropy);
  stats->entropy += entropy;
  stats->edit_fraction_on_max_pov += (double)count_on_max / (double)count_revisions;
  stats->edits_on_max_pov += (double)count_on_max;
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  double max_pov_antagonism = controversy_by_pov[max_topic * revision_assignment_header->pov_per_topic
						 + max_pov];
  stats->max_pov_antagonism += max_pov_antagonism;
  stats->max_pov_antagonism_edit_fraction += max_pov_antagonism 
    * (double)count_on_max / (double)count_revisions;

  double max_topic_antagonism = 0.0;
  for (int pov_num = 0; pov_num < revision_assignment_header->pov_per_topic; ++pov_num) {
    max_topic_antagonism += controversy_by_pov[max_topic * revision_assignment_header->pov_per_topic + pov_num];
  }
  max_topic_antagonism /= (double)(revision_assignment_header->pov_per_topic);
  stats->max_topic_antagonism += max_topic_antagonism;
  stats->max_topic_antagonism_pov_edit_fraction += max_topic_antagonism
    * (double)count_on_max / (double)count_revisions;

  struct topic_summary* max_topic_summary;
  get_topic_summary(mmap_info, max_topic, &max_topic_summary, NULL, NULL);
  stats->max_topic_rv_general += (double)(max_topic_summary->revert_general_count) 
    / (double)(max_topic_summary->norevert_general_count + max_topic_summary->revert_general_count);
  stats->max_topic_rv_topic += (double)(max_topic_summary->revert_topic_count) 
    / (double)(max_topic_summary->norevert_topic_count + max_topic_summary->revert_topic_count);

  int64_t num_pov_reverts;
  int64_t num_pov_reverted;
  int64_t num_reverts;
  count_pov_reverts(mmap_info, revision_ids, count_revisions,
		    &num_pov_reverts, &num_pov_reverted, &num_reverts);
  if (num_reverts != 0) {
    stats->pov_revert_revert_fraction += (double)num_pov_reverts / (double)num_reverts;
  }
  stats->pov_revert_edit_fraction += (double)num_pov_reverts / (double)count_revisions;
  stats->pov_reverted_edit_fraction += (double)num_pov_reverted / (double)count_revisions;
  stats->pov_reverts += (double)num_reverts;
  stats->edits += (double)count_revisions;
}

struct user_pair_stats* read_user_pairs(const char* file_name, int64_t* count_pairs) {
  FILE* user_pairs_in = fopen(file_name, "r");
  int64_t first_user;
  int64_t second_user;
  *count_pairs = 0;
  while (fscanf(user_pairs_in, "%"PRId64" %"PRId64"\n", &first_user, &second_user) != EOF) {
    ++(*count_pairs);
  }
  struct user_pair_stats* ret = calloc(*count_pairs, 
				       sizeof(struct user_pair_stats));
  rewind(user_pairs_in);
  int64_t current_comparison = 0;
  while (fscanf(user_pairs_in, "%"PRId64" %"PRId64"\n",
		&(ret[current_comparison].first_user),
		&(ret[current_comparison].second_user)) != EOF) {
    ++current_comparison;
  }
  fclose(user_pairs_in);
  return ret;
}

int main(int argc, char **argv) {
  if (argc < NON_VAR_ARGS + 1) {
    printf("Usage: %s mmap_directory threads user_pairs_file saved_assignment1 [saved_assignment2, ...]\n",
           argv[0]);
    exit(1);
  }
  // Nothing we do will be committed back to the files
  struct mmap_info mmap_info = open_mmaps_readonly(argv[1]);
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info.revision_assignment_mmap;
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info.topic_index_mmap;
  struct user_topic_header* user_topic_header
    = (struct user_topic_header*)mmap_info.user_topic_mmap;
  int num_threads = atoi(argv[2]);
  int count_assignments = argc - NON_VAR_ARGS;
  int64_t count_pairs;
  struct user_pair_stats* user_pair_stats = read_user_pairs(argv[3], &count_pairs);
  struct sample_threads sample_threads;
  initialize_threads(&sample_threads, num_threads, &mmap_info);
  double* controversy_by_pov 
    = (double*)malloc(sizeof(double) * revision_assignment_header->num_topics
		      * revision_assignment_header->pov_per_topic);
  struct page_user_stats* page_stats
    = calloc(topic_summary_header->num_pages, sizeof(struct page_user_stats));
  struct page_user_stats* user_stats
    = calloc(user_topic_header->num_users, sizeof(struct page_user_stats));
  struct pov_workspace* pov_workspaces = calloc(num_threads, sizeof(struct pov_workspace));
  for (int i = 0; i < num_threads; ++i) {
    init_pov_workspace(&mmap_info, pov_workspaces + i);
  }
  for (int assignment_n = 0; assignment_n < count_assignments; ++assignment_n) {
    const char* assignment_file = argv[NON_VAR_ARGS + assignment_n];
    int64_t assignment_mmap_size;
    char* assignments;
    if (assignment_n == 0 && strlen(assignment_file) == 1 && assignment_file[0] == '_') {
      assignment_mmap_size = -1;
      assignments = NULL;
    } else {
      assignments = open_mmap_read(assignment_file, &assignment_mmap_size);
      resample_restore(&sample_threads, 
		       (struct revision_assignment*)(assignments + sizeof(struct revision_assignment_header)));
    }
    all_pov_controversy(&mmap_info, controversy_by_pov);
    parallel_work(&mmap_info, pov_workspaces, controversy_by_pov,
		  user_stats, user_topic_header->num_users, num_threads,
		  update_user_stats);
    parallel_work(&mmap_info, pov_workspaces, controversy_by_pov,
		  page_stats, topic_summary_header->num_pages, num_threads,
		  update_page_stats);
    parallel_work(&mmap_info, pov_workspaces, controversy_by_pov,
		  user_pair_stats, count_pairs, num_threads,
		  update_user_antagonism);
    if (assignments != NULL) {
      munmap(assignments, assignment_mmap_size);
    }
  }
  FILE* users_file = fopen("users_stats.txt", "w");
  FILE* pages_file = fopen("pages_stats.txt", "w");
  FILE* user_comparisons_out = fopen("user_comparisons.txt", "w");
  for (int64_t userid = 0; userid < user_topic_header->num_users; ++userid) {
    print_stats(user_stats + userid, userid, count_assignments, users_file);
  }
  for (int64_t pageid = 0; pageid < topic_summary_header->num_pages; ++pageid) {
    print_stats(page_stats + pageid, pageid, count_assignments, pages_file);
  }
  for (int64_t pair_num = 0; pair_num < count_pairs; ++pair_num) {
    fprintf(user_comparisons_out,
	    "%"PRId64" %"PRId64" %lf\n", 
	    user_pair_stats[pair_num].first_user,
	    user_pair_stats[pair_num].second_user,
	    user_pair_stats[pair_num].antagonism / count_assignments);
  }
  fclose(user_comparisons_out);
  fclose(users_file);
  fclose(pages_file);
  for (int i = 0; i < num_threads; ++i) {
    free_pov_workspace(pov_workspaces + i);
  }
  free(controversy_by_pov);
  free(user_pair_stats);
  destroy_threads(&sample_threads);
  close_mmaps(mmap_info);
}

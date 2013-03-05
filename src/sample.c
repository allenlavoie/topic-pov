#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "index.h"
#include "probability.h"
#include "sample.h"

struct index_update {
  int64_t subtract_locations[4];
  int64_t add_locations[4];
};

/* Functions satisfying sample_thread_info.revision_callback */

void resample_initialize(struct sample_thread_info* thread_info, int64_t revision_id);
void resample_destroy(struct sample_thread_info* thread_info, int64_t revision_id);
void null_assignment(struct sample_thread_info* thread_info, int64_t revision_id);
void resample_assign(struct sample_thread_info* thread_info, int64_t revision_id);
void revision_transition_probability(struct sample_thread_info* thread_info, int64_t revision_id);
void resample_revision(struct sample_thread_info* thread_info, int64_t revision_id);

/* Functions satisfying sample_thread_info.index_update_function */

void apply_index_update(const struct mmap_info* mmap_info,
			const struct index_update* patch);
void apply_index_update_add(const struct mmap_info* mmap_info, 
			    const struct index_update* patch);
void apply_index_update_sub(const struct mmap_info* mmap_info, 
			    const struct index_update* patch);

/* Functions satisfying sample_thread_info.sample_function */
 
void sample_random(const double* sampling_array,
		   double probability_sum,
		   int num_topics, int pov_per_topic,
		   int* chosen_topic, int* chosen_pov,
		   gsl_rng* rand_gen);
void sample_maximize(const double* sampling_array,
		     double probability_sum,
		     int num_topics, int pov_per_topic,
		     int* chosen_topic, int* chosen_pov,
		     gsl_rng* rand_gen);

/* Other interal functions */

void reset_thread(struct sample_thread_info* thread_info);
double resample_internal(struct sample_threads* sample_threads,
			 void (*revision_callback) (struct sample_thread_info*, int64_t),
			 void (*index_update_function) (const struct mmap_info* mmap_info, 
							const struct index_update* patch),
			 void (*sample_function) (const double* sampling_array,
						  double probability_sum,
						  int num_topics, int pov_per_topic,
						  int* chosen_topic, int* chosen_pov,
						  gsl_rng* rand_gen),
			 int increment);
void initialize_sample_thread(const struct mmap_info* mmap_info,
			      pthread_mutex_t* queue_lock, 
			      pthread_rwlock_t* user_locks,
			      int seed_offset,
			      int sample_pages,
			      int mod_n,
			      struct index_update* index_update_queue,
			      int64_t* queue_location,
			      struct sample_thread_info* thread_info,
			      char* allocated_topic_dist);
void destroy_sample_thread(struct sample_thread_info* thread_info);

void resample_page(struct sample_thread_info* thread_info, int64_t page_id);
void* resample_pages_modn(void* tinfo);
void* log_likelihood_modn(void* tinfo);

void update_locations(const struct mmap_info* mmap_info,
		      int parent_topic, int parent_pov,
		      int topic, int pov,
		      int disagrees,
		      int64_t* first_update,
		      int64_t* second_update) {
  if (topic < 0 || pov < 0) {
    *first_update = offsetof(struct topic_summary_header, _dummy_var);
    *second_update = offsetof(struct topic_summary_header, _dummy_var);
    return;
  }
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)(mmap_info->topic_index_mmap);
  int64_t pov_size = sizeof(struct pov_summary) * topic_summary_header->pov_per_topic 
    * (topic_summary_header->pov_per_topic - 1);
  int64_t base = sizeof(struct topic_summary_header) 
    + topic * (pov_size + sizeof(struct topic_summary));
  *first_update = base + offsetof(struct topic_summary, total_revisions);
  if (parent_topic == topic) {
    if (parent_pov != pov) {
      if (parent_pov > pov) {
	parent_pov -= 1;
      }
      int64_t ant_location = base + sizeof(struct topic_summary)
	+ sizeof(struct pov_summary) * ((topic_summary_header->pov_per_topic - 1) 
					* pov + parent_pov);
      if (disagrees) {
	*second_update = ant_location + offsetof(struct pov_summary, revert_count);
      } else {
	*second_update = ant_location + offsetof(struct pov_summary, norevert_count);
      }
    } else {
      if (disagrees) {
	*second_update = base + offsetof(struct topic_summary, revert_topic_count);
      } else {
	*second_update = base + offsetof(struct topic_summary, norevert_topic_count);
      }
    }
  } else if (parent_topic >= 0 && parent_pov >= 0) {
    if (disagrees) {
      *second_update = base + offsetof(struct topic_summary, revert_general_count);
    } else {
      *second_update = base + offsetof(struct topic_summary, norevert_general_count);
    }
  } else {
    *second_update = offsetof(struct topic_summary_header, _dummy_var);
  }
}

void create_index_update(const struct mmap_info* mmap_info,
			 const struct index_patch* index_patch,
			 int new_topic, int new_pov,
			 struct index_update* index_update_out) {
  update_locations(mmap_info, index_patch->parent_topic, index_patch->parent_pov, 
		   index_patch->topic, index_patch->pov, index_patch->disagrees,
		   index_update_out->subtract_locations, 
		   index_update_out->subtract_locations + 1);
  update_locations(mmap_info, index_patch->topic, index_patch->pov, 
		   index_patch->child_topic, index_patch->child_pov, 
		   index_patch->child_disagrees,
		   index_update_out->subtract_locations + 2, 
		   index_update_out->subtract_locations + 3);

  update_locations(mmap_info, index_patch->parent_topic, index_patch->parent_pov, 
		   new_topic, new_pov, index_patch->disagrees,
		   index_update_out->add_locations, 
		   index_update_out->add_locations + 1);
  update_locations(mmap_info, new_topic, new_pov, index_patch->child_topic, 
		   index_patch->child_pov, index_patch->child_disagrees,
		   index_update_out->add_locations + 2, 
		   index_update_out->add_locations + 3);
}

void apply_index_update(const struct mmap_info* mmap_info, 
			const struct index_update* patch) {
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[0]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[1]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[2]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[3]) += 1;
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[0]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[1]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[2]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[3]) -= 1) >= 0);
}

void apply_index_update_sub(const struct mmap_info* mmap_info,
			    const struct index_update* patch) {
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[0]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[1]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[2]) -= 1) >= 0);
  assert((*(int64_t*)(mmap_info->topic_index_mmap 
		      + patch->subtract_locations[3]) -= 1) >= 0);
}

void apply_index_update_add(const struct mmap_info* mmap_info, 
			    const struct index_update* patch) {
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[0]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[1]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[2]) += 1;
  *(int64_t*)(mmap_info->topic_index_mmap + patch->add_locations[3]) += 1;
}

void resample(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, resample_revision, 
		    apply_index_update, sample_random, 1);
}

void resample_uniform(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, resample_initialize, 
		    apply_index_update_add, sample_random, 1);
}

void resample_null(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, null_assignment, 
		    apply_index_update_add, sample_random, 1);
}

void resample_zero(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, resample_destroy, 
		    apply_index_update_sub, sample_random, -1);
}

void resample_maximize(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, resample_revision, 
		    apply_index_update, sample_maximize, 1);
}

void resample_restore(struct sample_threads* sample_threads,
		      const struct revision_assignment* revision_assignments) {
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    sample_threads->thread_info[i].reference_assignments = revision_assignments;
  }
  resample_internal(sample_threads, resample_assign,
		    apply_index_update, sample_random, 1);
}

double transition_probability(struct sample_threads* sample_threads,
			    const struct revision_assignment* revision_assignments) {
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    sample_threads->thread_info[i].reference_assignments = revision_assignments;
  }
  return resample_internal(sample_threads, revision_transition_probability,
			   apply_index_update, sample_random, 1);
}

void resample_reverse(struct sample_threads* sample_threads) {
  resample_internal(sample_threads, resample_revision, 
		    apply_index_update, sample_random, -1);
}

double resample_internal(struct sample_threads* sample_threads,
			 void (*revision_callback) (struct sample_thread_info*, int64_t),
			 void (*index_update_function) (const struct mmap_info* mmap_info, 
							const struct index_update* patch),
			 void (*sample_function) (const double* sampling_array,
						  double probability_sum,
						  int num_topics, int pov_per_topic,
						  int* chosen_topic, int* chosen_pov,
						  gsl_rng* rand_gen),
			 int increment) {
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    sample_threads->thread_info[i].output = 0.0;
    sample_threads->thread_info[i].revision_callback = revision_callback;
    sample_threads->thread_info[i].index_update_function = index_update_function;
    sample_threads->thread_info[i].sample_function = sample_function;
    sample_threads->thread_info[i].increment = increment;
    pthread_create(&(sample_threads->thread_info[i].thread),
		   NULL, resample_pages_modn, 
		   (void*)(sample_threads->thread_info + i));
  }
  double ret = 0.0;
  void* res;
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    pthread_join(sample_threads->thread_info[i].thread, &res);
    ret += sample_threads->thread_info[i].output;
  }
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    reset_thread(sample_threads->thread_info + i);
  }
  sample_threads->queue_location = 0;
  return ret;
}

void* initialize_user_topics_modn(void* tinfo) {
  struct sample_thread_info* thread_info = (struct sample_thread_info*)tinfo;
  initialize_user_topics(&(thread_info->mmap_info), 
			 thread_info->sample_pages, thread_info->mod_n);
  return NULL;
}

void parallel_initialize_user_topics(struct sample_threads* sample_threads) {
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    pthread_create(&(sample_threads->thread_info[i].thread),
		   NULL, initialize_user_topics_modn, 
		   (void*)(sample_threads->thread_info + i));
  }
  void* res;
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    pthread_join(sample_threads->thread_info[i].thread, &res);
  }
}

double parallel_log_likelihood(struct sample_threads* sample_threads) {
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    pthread_create(&(sample_threads->thread_info[i].thread),
		   NULL, log_likelihood_modn, 
		   (void*)(sample_threads->thread_info + i));
  }
  void* res;
  double log_likelihood = log_likelihood_gamma(&(sample_threads->thread_info[0].mmap_info), 0);
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    pthread_join(sample_threads->thread_info[i].thread, &res);
    log_likelihood += *((double*)res);
    free(res);
  }
  return log_likelihood;
}

void* log_likelihood_modn(void* tinfo) {
  struct sample_thread_info* thread_info = (struct sample_thread_info*)tinfo;
  double* ret = malloc(sizeof(double));
  *ret = users_pages_probability_modn(&(thread_info->mmap_info), 
				thread_info->sample_pages, thread_info->mod_n);
  return (void*)ret;
}

int64_t topic_summary_size(const struct mmap_info* mmap_info) {
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)(mmap_info->topic_index_mmap);
  int32_t pov_size = sizeof(struct pov_summary) * topic_summary_header->pov_per_topic 
    * (topic_summary_header->pov_per_topic - 1);
  return sizeof(struct topic_summary_header) + (pov_size + sizeof(struct topic_summary)) 
    * topic_summary_header->num_topics;
}

void initialize_threads(struct sample_threads* sample_threads, int num_threads,
			const struct mmap_info* mmap_info) {
  assert(num_threads > 0);
  sample_threads->num_threads = num_threads;
  sample_threads->thread_info = malloc(sizeof(struct sample_thread_info) * num_threads);
  pthread_mutex_init(&(sample_threads->queue_lock), NULL);
  for (int i = 0; i < NUM_USER_LOCKS; ++i) {
    pthread_rwlock_init(sample_threads->user_locks + i, NULL);
  }
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  sample_threads->index_update_queue 
    = malloc(revision_assignment_header->count_revisions * sizeof(struct index_update));
  assert(sample_threads->index_update_queue != NULL);
  sample_threads->queue_location = 0;
  ((struct topic_summary_header*)mmap_info->topic_index_mmap)->_dummy_var
    = INT64_MAX / 2;
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    char *allocated_topic_dist = NULL;
    if (i != 0) {
      int64_t summary_size = topic_summary_size(mmap_info);
      allocated_topic_dist = malloc(summary_size);
      memcpy(allocated_topic_dist, mmap_info->topic_index_mmap, summary_size);
    }
    initialize_sample_thread(mmap_info, &(sample_threads->queue_lock),
			     sample_threads->user_locks,
			     i, i, num_threads, 
			     sample_threads->index_update_queue,
			     &(sample_threads->queue_location),
			     sample_threads->thread_info + i,
			     allocated_topic_dist);
  }
}

void destroy_threads(struct sample_threads* sample_threads) {
  pthread_mutex_destroy(&(sample_threads->queue_lock));
  for (int i = 0; i < NUM_USER_LOCKS; ++i) {
    pthread_rwlock_destroy(sample_threads->user_locks + i);
  }
  
  for (int i = 0; i < sample_threads->num_threads; ++i) {
    destroy_sample_thread(sample_threads->thread_info + i);
  }
  free(sample_threads->thread_info);
  free(sample_threads->index_update_queue);
}

double* allocate_sampling_array(const struct mmap_info* mmap_info) {
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  return (double*)malloc(sizeof(double) * revision_assignment_header->num_topics 
			 * revision_assignment_header->pov_per_topic);
}

// Requires write lock on queue!
void push_queue(struct sample_thread_info* thread_info,
		const struct index_update* index_update) {
  *(thread_info->index_update_queue + *(thread_info->queue_location)) = *index_update;
  *(thread_info->queue_location) += 1;
}

// Does not require a read lock on queue
// (but you may need one to get a to_position consistent with other data).
// Read thread_info->last_queue_position up to but not including to_position
void pop_queue(struct sample_thread_info* thread_info,
	       int64_t to_position) {
  for (; thread_info->last_queue_position < to_position; 
       ++(thread_info->last_queue_position)) {
    thread_info->index_update_function(&(thread_info->mmap_info), 
				       thread_info->index_update_queue 
				       + thread_info->last_queue_position);
  }
}

void reset_thread(struct sample_thread_info* thread_info) {
  pop_queue(thread_info, *(thread_info->queue_location));
  thread_info->last_queue_position = 0;
  ((struct topic_summary_header*)thread_info->mmap_info.topic_index_mmap)->_dummy_var
    = INT64_MAX / 2;
  thread_info->increment = 1;
  thread_info->reference_assignments = NULL;
  thread_info->output = 0.0;
}

void initialize_sample_thread(const struct mmap_info* mmap_info,
			      pthread_mutex_t* queue_lock, 
			      pthread_rwlock_t* user_locks,
			      int seed_offset,
			      int sample_pages,
			      int mod_n,
			      struct index_update* index_update_queue,
			      int64_t* queue_location,
			      struct sample_thread_info* thread_info,
			      char* allocated_topic_dist) {
  thread_info->user_locks = user_locks;
  thread_info->queue_lock = queue_lock;
  thread_info->rand_gen = gsl_rng_alloc(gsl_rng_default);
  gsl_rng_set(thread_info->rand_gen, time(NULL) + seed_offset);
  thread_info->sampling_array = allocate_sampling_array(mmap_info);
  memcpy(&(thread_info->mmap_info), mmap_info, sizeof(struct mmap_info));
  if (allocated_topic_dist != NULL) {
    thread_info->mmap_info.topic_index_mmap = allocated_topic_dist;
  }
  thread_info->allocated_topic_dist = allocated_topic_dist;
  thread_info->sample_pages = sample_pages;
  thread_info->mod_n = mod_n;
  thread_info->last_queue_position = 0;
  thread_info->queue_location = queue_location;
  thread_info->index_update_queue = index_update_queue;
  thread_info->increment = 1;
  thread_info->reference_assignments = NULL;
}

void destroy_sample_thread(struct sample_thread_info* thread_info) {
  gsl_rng_free(thread_info->rand_gen);
  free(thread_info->sampling_array);
  thread_info->sampling_array = NULL;
  thread_info->rand_gen = NULL;
  thread_info->queue_lock = NULL;
  thread_info->user_locks = NULL;
  free(thread_info->allocated_topic_dist);
  thread_info->allocated_topic_dist = NULL;
}

void sample_random(const double* sampling_array,
		   double probability_sum,
		   int num_topics, int pov_per_topic,
		   int* chosen_topic, int* chosen_pov,
		   gsl_rng* rand_gen) {
  double chosen = probability_sum * gsl_rng_uniform(rand_gen);
  double running_sum = 0.0;
  for (int topic = 0; topic < num_topics; ++topic) {
    for (int pov = 0; pov < pov_per_topic; ++pov) {
      running_sum += sampling_array[topic * pov_per_topic + pov];
      if (chosen < running_sum) {
	*chosen_topic = topic;
	*chosen_pov = pov;
	return;
      }
    }
  }
}

void sample_maximize(const double* sampling_array,
		     double probability_sum,
		     int num_topics, int pov_per_topic,
		     int* chosen_topic, int* chosen_pov,
		     gsl_rng* rand_gen) {
  double max_prob = 0.0;
  for (int topic = 0; topic < num_topics; ++topic) {
    for (int pov = 0; pov < pov_per_topic; ++pov) {
      if (sampling_array[topic * pov_per_topic + pov] > max_prob) {
	max_prob = sampling_array[topic * pov_per_topic + pov];
	*chosen_topic = topic;
	*chosen_pov = pov;
      }
    }
  }
}

void resample_revision(struct sample_thread_info* thread_info, int64_t revision_id) {
  struct index_patch index_patch;
  int64_t queue_location;
  
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)(thread_info->mmap_info
					   .revision_assignment_mmap);
  const struct revision* revision 
    = get_revision(&(thread_info->mmap_info), revision_id);
  double* user_topic_pov_dist;
  get_user_topics(&(thread_info->mmap_info), revision->user, &user_topic_pov_dist);
  // Patch this revision out of indexes temporarily;
  // pseudo-counts should not take it into account
  fill_index_patch(&(thread_info->mmap_info), revision_id, &index_patch);
  pthread_rwlock_rdlock(thread_info->user_locks + (revision->user % NUM_USER_LOCKS));
  // Read queue position
  queue_location = *(thread_info->queue_location);
  // Copy user distribution
  memcpy(thread_info->sampling_array, user_topic_pov_dist,
	 sizeof(double) * revision_assignment_header->num_topics 
	 * revision_assignment_header->pov_per_topic);
  pthread_rwlock_unlock(thread_info->user_locks + (revision->user % NUM_USER_LOCKS));

  // Update distribution from queue
  pop_queue(thread_info, queue_location);
  assert(index_patch.topic >= 0 && index_patch.pov >= 0);
  double probability_sum = 0.0;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
      int array_location = topic * revision_assignment_header->pov_per_topic + pov;
      if (topic == index_patch.topic && pov == index_patch.pov) {
	assert((thread_info
		->sampling_array[array_location] -= 1.0) >= 0);
      }
      thread_info->sampling_array[array_location]
	*= revision_probability(&(thread_info->mmap_info), revision_id, topic, 
				pov, 1, 0, &index_patch);
      probability_sum += thread_info->sampling_array[array_location];
    }
  }
  assert(probability_sum > 0.0);
  int chosen_topic = -1;
  int chosen_pov = -1;
  thread_info->sample_function(thread_info->sampling_array,
			       probability_sum,
			       revision_assignment_header->num_topics,
			       revision_assignment_header->pov_per_topic,
			       &chosen_topic,
			       &chosen_pov,
			       thread_info->rand_gen);
  assert(chosen_topic != -1);
  assert(chosen_pov != -1);
  
  if (chosen_topic != index_patch.topic || chosen_pov != index_patch.pov) {
    struct revision_assignment* revision_assignment 
      = get_revision_assignment(&(thread_info->mmap_info), revision_id);
    struct index_update index_update;
    create_index_update(&(thread_info->mmap_info), &index_patch, chosen_topic,
			chosen_pov, &index_update);
    // This assignment will only be referenced on this page, 
    // so this update is not a critical section.
    revision_assignment->pov = chosen_pov;
    revision_assignment->topic = chosen_topic;
    pthread_rwlock_wrlock(thread_info->user_locks
			  + (revision->user % NUM_USER_LOCKS));
    pthread_mutex_lock(thread_info->queue_lock);
    // Add this update to the queue
    push_queue(thread_info, &index_update);
    // We'll patch our indexes when we read it out
    pthread_mutex_unlock(thread_info->queue_lock);
    // Update the user distribution
    assert((user_topic_pov_dist[index_patch.topic
				* revision_assignment_header->pov_per_topic
				+ index_patch.pov] -= 1.0) >= 0.0);
    user_topic_pov_dist[chosen_topic * revision_assignment_header->pov_per_topic
			+ chosen_pov] += 1.0;
    pthread_rwlock_unlock(thread_info->user_locks
			  + (revision->user % NUM_USER_LOCKS));
    // Update page distribution. We're sampling the whole page in this thread,
    // so this is not a critical section
    int64_t* page_dist;
    get_topic_summary(&(thread_info->mmap_info), index_patch.topic, 
		      NULL, NULL, &page_dist);
    page_dist[revision->article] -= 1;
    get_topic_summary(&(thread_info->mmap_info), chosen_topic, 
		      NULL, NULL, &page_dist);
    page_dist[revision->article] += 1;
  }
}

void revision_transition_probability(struct sample_thread_info* thread_info,
				     int64_t revision_id) {
  struct index_patch index_patch;
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)(thread_info->mmap_info
					   .revision_assignment_mmap);
  const struct revision* revision 
    = get_revision(&(thread_info->mmap_info), revision_id);
  double* user_topic_pov_dist;
  get_user_topics(&(thread_info->mmap_info), revision->user, &user_topic_pov_dist);
  // Patch this revision out of indexes temporarily;
  // pseudo-counts should not take it into account
  fill_index_patch(&(thread_info->mmap_info), revision_id, &index_patch);
  // We're not updating, so no need to lock
  memcpy(thread_info->sampling_array, user_topic_pov_dist,
	 sizeof(double) * revision_assignment_header->num_topics 
	 * revision_assignment_header->pov_per_topic);

  double probability_sum = 0.0;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
      if (topic == index_patch.topic && pov == index_patch.pov) {
	assert((thread_info
		->sampling_array[topic * revision_assignment_header->pov_per_topic 
				 + pov] -= 1.0) >= 0);
      }
      probability_sum 
	+= (thread_info->sampling_array[topic 
					* revision_assignment_header->pov_per_topic 
					+ pov]
	    *= revision_probability(&(thread_info->mmap_info), revision_id, topic, 
				    pov, 1, 0, &index_patch));
    }
  }
  assert(probability_sum > 0.0);
  const struct revision_assignment* reference_assignment
    = thread_info->reference_assignments + revision_id;
  thread_info->output
    += log(thread_info->sampling_array[reference_assignment->topic
				       * revision_assignment_header->pov_per_topic
				       + reference_assignment->pov])
    - log(probability_sum);
}

void resample_assign(struct sample_thread_info* thread_info, int64_t revision_id) {
  int64_t queue_location;
  
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)(thread_info->mmap_info
					   .revision_assignment_mmap);
  const struct revision* revision 
    = get_revision(&(thread_info->mmap_info), revision_id);
  // Store the current assignments
  struct index_patch index_patch;
  fill_index_patch(&(thread_info->mmap_info), revision_id, &index_patch);
  // Read queue position
  queue_location = *(thread_info->queue_location);
  // Update distribution from queue
  pop_queue(thread_info, queue_location);

  int chosen_topic = thread_info->reference_assignments[revision_id].topic;
  int chosen_pov = thread_info->reference_assignments[revision_id].pov;

  if (chosen_topic != index_patch.topic || chosen_pov != index_patch.pov) {
    double* user_topic_pov_dist;
    get_user_topics(&(thread_info->mmap_info), revision->user, &user_topic_pov_dist);
    struct revision_assignment* revision_assignment 
      = get_revision_assignment(&(thread_info->mmap_info), revision_id);
    struct index_update index_update;
    create_index_update(&(thread_info->mmap_info), &index_patch, chosen_topic,
			chosen_pov, &index_update);
    // This assignment will only be referenced on this page, 
    // so this update is not a critical section.
    revision_assignment->pov = chosen_pov;
    revision_assignment->topic = chosen_topic;
    pthread_rwlock_wrlock(thread_info->user_locks
			  + (revision->user % NUM_USER_LOCKS));
    pthread_mutex_lock(thread_info->queue_lock);
    // Add this update to the queue
    push_queue(thread_info, &index_update);
    // We'll patch our indexes when we read it out
    pthread_mutex_unlock(thread_info->queue_lock);
    // Update the user distribution
    assert((user_topic_pov_dist[index_patch.topic
				* revision_assignment_header->pov_per_topic
				+ index_patch.pov] -= 1.0) >= 0.0);
    user_topic_pov_dist[chosen_topic * revision_assignment_header->pov_per_topic
			+ chosen_pov] += 1.0;
    pthread_rwlock_unlock(thread_info->user_locks
			  + (revision->user % NUM_USER_LOCKS));
    // Update page distribution. We're sampling the whole page in this thread,
    // so this is not a critical section
    int64_t* page_dist;
    get_topic_summary(&(thread_info->mmap_info), index_patch.topic, 
		      NULL, NULL, &page_dist);
    page_dist[revision->article] -= 1;
    get_topic_summary(&(thread_info->mmap_info), chosen_topic, 
		      NULL, NULL, &page_dist);
    page_dist[revision->article] += 1;
  }
}

void null_assignment(struct sample_thread_info* thread_info, int64_t revision_id) {
  struct revision_assignment* revision_assignment 
    = get_revision_assignment(&(thread_info->mmap_info), revision_id);
  revision_assignment->topic = -1;
  revision_assignment->pov = -1;  
}

void resample_initialize(struct sample_thread_info* thread_info, int64_t revision_id) {
  int64_t queue_location;
  
  const struct revision* revision
    = get_revision(&(thread_info->mmap_info), revision_id);
  struct revision_assignment* revision_assignment 
    = get_revision_assignment(&(thread_info->mmap_info), revision_id);

  if (revision->parent >= 0) {
    struct revision_assignment* parent_assignment
      = get_revision_assignment(&(thread_info->mmap_info), revision->parent);
    if (parent_assignment->pov < 0 || parent_assignment->topic < 0) {
      assert(revision->parent > revision_id);
      // Revision IDs are out of order, so we need to delay this assignment
      // until we get to its parent, then come back to it.
      // Otherwise the parent should have been initialized first.
      return;
    }
  }
  
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)(thread_info->mmap_info
					   .revision_assignment_mmap);
  // Store the current assignments
  struct index_patch index_patch;
  fill_index_patch(&(thread_info->mmap_info), revision_id, &index_patch);
  index_patch.child_topic = -1;
  index_patch.child_pov = -1;
  // Read queue position
  queue_location = *(thread_info->queue_location);
  // Update distribution from queue
  pop_queue(thread_info, queue_location);

  int chosen_topic = gsl_rng_uniform_int(thread_info->rand_gen,
					 revision_assignment_header->num_topics);
  int chosen_pov = gsl_rng_uniform_int(thread_info->rand_gen,
				       revision_assignment_header->pov_per_topic);
  
  struct index_update index_update;
  create_index_update(&(thread_info->mmap_info), &index_patch, chosen_topic,
		      chosen_pov, &index_update);
  double* user_topic_pov_dist;
  get_user_topics(&(thread_info->mmap_info), revision->user, &user_topic_pov_dist);
  pthread_rwlock_wrlock(thread_info->user_locks
			+ (revision->user % NUM_USER_LOCKS));
  pthread_mutex_lock(thread_info->queue_lock);
  revision_assignment->pov = chosen_pov;
  revision_assignment->topic = chosen_topic;
  // Add this update to the queue
  push_queue(thread_info, &index_update);
  // We'll patch our indexes when we read it out
  pthread_mutex_unlock(thread_info->queue_lock);
  // Update the user distribution
  user_topic_pov_dist[chosen_topic * revision_assignment_header->pov_per_topic
		      + chosen_pov] += 1.0;
  pthread_rwlock_unlock(thread_info->user_locks
			+ (revision->user % NUM_USER_LOCKS));
  // Update page distribution. We're sampling the whole page in this thread,
  // so this is not a critical section
  int64_t* page_dist;
  get_topic_summary(&(thread_info->mmap_info), chosen_topic, 
		    NULL, NULL, &page_dist);
  page_dist[revision->article] += 1;
  if (revision->child < revision_id && revision->child >= 0) {
    struct revision_assignment* child_assignment
      = get_revision_assignment(&(thread_info->mmap_info), revision->child);
    assert(child_assignment->topic < 0 && child_assignment->pov < 0);
    // We skipped this assignment previously because its parent was not yet
    // assigned. Now we go back and do the assignment.
    resample_initialize(thread_info, revision->child);
  }
}

void resample_destroy(struct sample_thread_info* thread_info, int64_t revision_id) {
  int64_t queue_location;
  
  const struct revision* revision
    = get_revision(&(thread_info->mmap_info), revision_id);
  struct revision_assignment* revision_assignment 
    = get_revision_assignment(&(thread_info->mmap_info), revision_id);
  
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)(thread_info->mmap_info
					   .revision_assignment_mmap);
  // Store the current assignments
  struct index_patch index_patch;
  fill_index_patch(&(thread_info->mmap_info), revision_id, &index_patch);
  index_patch.child_topic = -1;
  index_patch.child_pov = -1;
  // Read queue position
  queue_location = *(thread_info->queue_location);
  // Update distribution from queue
  pop_queue(thread_info, queue_location);

  int chosen_topic = -1;
  int chosen_pov = -1;
  
  struct index_update index_update;
  create_index_update(&(thread_info->mmap_info), &index_patch, chosen_topic,
		      chosen_pov, &index_update);
  double* user_topic_pov_dist;
  get_user_topics(&(thread_info->mmap_info), revision->user, &user_topic_pov_dist);
  pthread_rwlock_wrlock(thread_info->user_locks
			+ (revision->user % NUM_USER_LOCKS));
  pthread_mutex_lock(thread_info->queue_lock);
  revision_assignment->pov = chosen_pov;
  revision_assignment->topic = chosen_topic;
  // Add this update to the queue
  push_queue(thread_info, &index_update);
  // We'll patch our indexes when we read it out
  pthread_mutex_unlock(thread_info->queue_lock);
  // Update the user distribution
  assert((user_topic_pov_dist[index_patch.topic * revision_assignment_header->pov_per_topic
			      + index_patch.pov] -= 1.0) >= 0);
  pthread_rwlock_unlock(thread_info->user_locks
			+ (revision->user % NUM_USER_LOCKS));
  // Update page distribution. We're sampling the whole page in this thread,
  // so this is not a critical section
  int64_t* page_dist;
  get_topic_summary(&(thread_info->mmap_info), index_patch.topic, 
		    NULL, NULL, &page_dist);
  assert((page_dist[revision->article] -= 1) >= 0);
}

void resample_page(struct sample_thread_info* thread_info, int64_t page_id) {
  int64_t count_revisions;
  const int64_t* revision_ids;
  get_page(&(thread_info->mmap_info), page_id, &count_revisions, &revision_ids);
  if (thread_info->increment >= 0) {
    for (int64_t i = 0; i < count_revisions; ++i) {
      thread_info->revision_callback(thread_info, revision_ids[i]);
    }
  } else {
    for (int64_t i = count_revisions - 1; i >= 0; --i) {
      thread_info->revision_callback(thread_info, revision_ids[i]);
    }
  }
}

void* resample_pages_modn(void* tinfo) {
  struct sample_thread_info* thread_info = (struct sample_thread_info*)tinfo;
  int64_t num_pages = ((const struct page_header*)(thread_info->mmap_info.page_mmap))->count_pages;
  if (thread_info->increment >= 0) {
    for (int64_t page_number = thread_info->sample_pages; page_number < num_pages; 
	 page_number += thread_info->mod_n) {
      resample_page(thread_info, page_number);
    }
  } else {
    int64_t start_page = num_pages
      - (num_pages % thread_info->mod_n) 
      + thread_info->sample_pages;
    if (start_page >= num_pages) {
      start_page -= thread_info->mod_n;
    }
    for (int64_t page_number = start_page; 
	 page_number >= thread_info->sample_pages; 
	 page_number -= thread_info->mod_n) {
      resample_page(thread_info, page_number);
    }
  }
  return NULL;
}

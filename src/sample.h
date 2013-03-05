#ifndef __SAMPLE_H__
#define __SAMPLE_H__

#include <gsl/gsl_rng.h>
#include <pthread.h>
#include <stdint.h>

#include "parse_mmaps.h"

#define NUM_USER_LOCKS 2000

struct sample_thread_info;
struct index_update;
struct sample_threads;

/* Thread utility functions */

void initialize_threads(struct sample_threads* sample_threads, int num_threads,
			const struct mmap_info* mmap_info);
void destroy_threads(struct sample_threads* sample_threads);

/* Resampling functions */

/* Set all topic and POV assignments to -1, not doing any index
   updates. Used before initializing topics and POVs.*/
void resample_null(struct sample_threads* sample_threads);
/* Initialize topic and POV assignments by sampling them uniformly at
   random and performing index updates. */
void resample_uniform(struct sample_threads* sample_threads);

/* Re-sample topics and POVs. One iteration of Gibbs sampling. */
void resample(struct sample_threads* sample_threads);
/* Re-sample assignments, but (approximately) in the opposite order as
   resample(). Used during model selection. */
void resample_reverse(struct sample_threads* sample_threads);

/* Rather than re-sampling randomly, always choose the maximum
   probability assignment. Useful for finding a high probability
   assignment of topics and POVs after random sampling. */
void resample_maximize(struct sample_threads* sample_threads);
/* Restore a set of assignments, updating indexes as necessary */
void resample_restore(struct sample_threads* sample_threads, 
		      const struct revision_assignment* revision_assignments);
/* Set all topic and POV assignments to -1, subtracting the
   assignments from indexes. Primarily useful for verifying that
   indexes were correct. */
void resample_zero(struct sample_threads* sample_threads);

/* Parallel probability computations */

/* Compute the log probability of transitioning to the specified
   assignments from the current assignments (don't actually transition
   to them). */
double transition_probability(struct sample_threads* sample_threads,
			      const struct revision_assignment* revision_assignments);
/* Compute the likelihood of the current model and topic/POV
   assignments */
double parallel_log_likelihood(struct sample_threads* sample_threads);

/* Other miscellaneous parallel utility routines */

/* Set all of the user topic/POV distributions to alpha. Used during
   initialization. */
void parallel_initialize_user_topics(struct sample_threads* sample_threads);

/* Structs to hold synchronization and thread information. */

struct sample_thread_info {
  /* Each thread gets its own random number generator, initialized
     with a different seed. */
  gsl_rng* rand_gen;

  /* A num_topics * num_povs_per_topic array which threads use to
     sample new topic and POV assignments. */
  double* sampling_array;

  /* Each thread gets its own copy of mmap_info, allowing us to
     replace topic_index_mmap with a private copy which is manually
     synchronized. */
  struct mmap_info mmap_info;

  // Sample only pages numbered (page# % mod_n) == sample_pages
  int sample_pages;
  int mod_n;

  // Information about the current thread.
  pthread_t thread;

  /* An array of locks to synchronize access and updates to user
     topic/POV distributions. For a given user's topic and POV
     distribution, the lock to use is userid % NUM_USER_LOCKS. */
  pthread_rwlock_t* user_locks;

  /* Memory accounting for the topic_index_mmap copy, if any. One
     thread updates the original, and so does not have a specially
     allocated topic index, in which case allocated_topic_dist will be
     NULL. */
  char* allocated_topic_dist;

  /* The index of the first position in the topic index update queue
     that we have not read. */
  int64_t last_queue_position;

  /* Pointers to the global topic index update queue, and the global
     position of the first empty slot in the queue. Access to the
     queue location is synchronized with queue_lock. */
  struct index_update* index_update_queue;
  int64_t* queue_location;
  pthread_mutex_t* queue_lock;

  /* Generally +1 or -1, indicating whether sampling is forward or
     backward. */
  int increment;

  /* Helper functions to specify sampling behavior, replacing small or
     large parts of the sampling routine. */
  void (*revision_callback) (struct sample_thread_info*, int64_t);
  void (*index_update_function) (const struct mmap_info* mmap_info, 
				 const struct index_update* patch);
  void (*sample_function) (const double* sampling_array,
			   double probability_sum,
			   int num_topics, int pov_per_topic,
			   int* chosen_topic, int* chosen_pov,
			   gsl_rng* rand_gen);

  /* When computing transition probabilities, these assignments
     specify which assignments the transition is to. */
  const struct revision_assignment* reference_assignments;

  double output;
};

struct sample_threads {
  struct sample_thread_info* thread_info;
  pthread_mutex_t queue_lock;
  pthread_rwlock_t user_locks[NUM_USER_LOCKS];
  int num_threads;

  struct index_update* index_update_queue;
  // One past the last value in the queue
  int64_t queue_location;
};

#endif

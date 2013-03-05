/* Functions for computing probabilities under the model. Useful for
   sampling and for computing likelihoods. */

#ifndef __PROBABILITY_H__
#define __PROBABILITY_H__

/* Sometimes, usually when sampling, it is necessary temporarily
   "patch out" a specific assignment. This struct provides information
   about which assignment is being temporarily disregarded. Also used
   to temporarily store information about a revision. */
struct index_patch {
  int parent_topic;
  int parent_pov;

  int topic;
  int pov;
  int disagrees;

  int child_topic;
  int child_pov;
  int child_disagrees;

  int64_t page;
  int64_t user;
};

struct mmap_info;
struct revision_assignment;

/* Log likelihood functions */

/* Compute the log likelihood of current assignments without
   parallelism. */
double log_likelihood(const struct mmap_info* mmap_info);

/* Compute the log likelihood of all disagreements, optionally
   including page and (topic, POV) selection probabilities. To compute
   the page and (topic, POV) selection probabilities in parallel,
   users_pages_probability_modn must be called separately. */
double log_likelihood_gamma(const struct mmap_info* mmap_info, int include_users_pages);

/* Evaluate the log likelihood of the current assignments of topics
   and POVs across pages and users such that id % modn ==
   sample. Useful for parallel log likelihood evaluations. */
double users_pages_probability_modn(const struct mmap_info* mmap_info, int sample, int modn);

/* Sampling helper functions */

/* Compute the total probability of the given revision being observed,
   including the probability of the parent reference (disagreement or
   not), the probability of the observed page given the topic
   assignment, optionally including the probability of the child
   revision (disagreement or not), and optionally including the
   probability of the user selecting the given topic and POV. */
double revision_probability(const struct mmap_info* mmap_info, int64_t revision_id,
			    int32_t topic, int32_t pov, 
			    int include_child, int include_user_topic_pov,
			    const struct index_patch* index_patch);

/* Compute the probability of the revision "child" disagreeing (or
   not) with "parent" given their assignments and the current
   assignments of all other revisions, exluding the assignment
   described by index_patch. */
double reference_probability(const struct mmap_info* mmap_info, 
			     struct revision_assignment* parent,
			     struct revision_assignment* child,
			     int disagrees,
			     const struct index_patch* index_patch);

/* Fill the specified index patch with information about the topic and
   POV assignments of the specified revision, its parent, and child
   (if they exist). */
void fill_index_patch(const struct mmap_info* mmap_info, 
		      int64_t revision_id,
		      struct index_patch* index_patch);

#endif

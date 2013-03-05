#include <assert.h>
#include <gsl/gsl_sf_gamma.h>
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>

#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"

double reference_probability(const struct mmap_info* mmap_info, 
			     struct revision_assignment* parent,
			     struct revision_assignment* child,
			     int disagrees,
			     const struct index_patch* index_patch) {
  double action_prob = 1.0;
  if (parent != NULL && parent->topic >= 0 && parent->pov >= 0
      && child->topic >= 0 && child->pov >= 0) {
    struct revision_assignment_header* revision_assignment_header
      = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
    struct topic_summary* topic_summary;
    struct pov_summary* pov_dist;
    get_topic_summary(mmap_info, child->topic, &topic_summary, &pov_dist, NULL);
    if (parent->topic == child->topic) {
      if (parent->pov != child->pov) {
	// POV reference
	struct pov_summary* pov_summary = get_ant_pov(mmap_info, pov_dist, 
						      child->pov, parent->pov);
	double denom = (double)(pov_summary->norevert_count 
				+ pov_summary->revert_count)
	  + revision_assignment_header->psi_alpha
	  + revision_assignment_header->psi_beta;
	int correction = 0;
	if (index_patch != NULL
	    && index_patch->topic == child->topic
	    && index_patch->pov == child->pov
	    && index_patch->parent_pov == parent->pov
	    && index_patch->parent_topic == index_patch->topic) {
	  assert(denom -= 1.0 >= 0.0);
	  if (!disagrees == !(index_patch->disagrees)) {
	    correction += 1;
	  }
	}
	if (index_patch != NULL
	    && index_patch->child_topic == child->topic
	    && index_patch->child_pov == child->pov
	    && index_patch->pov == parent->pov
	    && index_patch->topic == index_patch->child_topic) {
	  assert(denom -= 1.0 >= 0.0);
	  if (!disagrees == !(index_patch->child_disagrees)) {
	    correction += 1;
	  }
	}
	if (disagrees) {
	  action_prob *= ((double)(pov_summary->revert_count 
				   - correction)
			  + revision_assignment_header->psi_alpha) / denom;
	} else {
	  action_prob *= ((double)(pov_summary->norevert_count
				   - correction)
			  + revision_assignment_header->psi_beta) / denom;
	}
      } else {
	// topic reference
	double denom = (double)(topic_summary->revert_topic_count
				+ topic_summary->norevert_topic_count
				+ revision_assignment_header->gamma_alpha 
				+ revision_assignment_header->gamma_beta);
	int correction = 0;
	if (index_patch != NULL
	    && index_patch->topic == child->topic
	    && index_patch->parent_pov == index_patch->pov
	    && index_patch->parent_topic == index_patch->topic) {
	  assert(denom -= 1.0 >= 0.0);
	  if (!disagrees == !(index_patch->disagrees)) {
	    correction += 1;
	  }
	}
	if (index_patch != NULL
	    && index_patch->child_topic == child->topic
	    && index_patch->pov == index_patch->child_pov
	    && index_patch->topic == index_patch->child_topic) {
	  assert(denom -= 1.0 >= 0.0);
	  if (!disagrees == !(index_patch->child_disagrees)) {
	    correction += 1;
	  }
	}
	if (disagrees) {
	  action_prob *= ((double)(topic_summary->revert_topic_count 
				   - correction)
			  + revision_assignment_header->gamma_alpha) / denom;
	} else {
	  action_prob *= ((double)(topic_summary->norevert_topic_count
				   - correction)
			  + revision_assignment_header->gamma_beta) / denom;
	}
      }
    } else {
      // general reference
      double denom = (double)(topic_summary->revert_general_count
			      + topic_summary->norevert_general_count)
	+ revision_assignment_header->gamma_alpha 
	+ revision_assignment_header->gamma_beta;
      int correction = 0;
      if (index_patch != NULL
	  && index_patch->parent_topic >= 0
	  && index_patch->topic == child->topic
	  && index_patch->parent_topic != index_patch->topic) {
	assert(denom -= 1.0 >= 0.0);
	if (!disagrees == !(index_patch->disagrees)) {
	  correction += 1;
	}
      }
      if (index_patch != NULL
	  && index_patch->topic >= 0
	  && index_patch->child_topic == child->topic
	  && index_patch->topic != index_patch->child_topic) {
	assert(denom -= 1.0 >= 0.0);
	if (!disagrees == !(index_patch->child_disagrees)) {
	  correction = 1;
	}
      }
      if (disagrees) {
	action_prob *= ((double)(topic_summary->revert_general_count 
				 - correction) 
			+ revision_assignment_header->gamma_alpha) / denom;
      } else {
	action_prob *= ((double)(topic_summary->norevert_general_count 
				 - correction)
			+ revision_assignment_header->gamma_beta) / denom;
      }
    }
  } // Else, a constant not dependant on POV or topic
  assert(action_prob > 0.0);
  return action_prob;
}

double revision_probability(const struct mmap_info* mmap_info, int64_t revision_id,
			    int32_t topic, int32_t pov, 
			    int include_child,
			    int include_user_topic_pov,
			    const struct index_patch* index_patch) {
  const struct revision* revision = get_revision(mmap_info, revision_id);
  struct revision_assignment current_assignment;
  current_assignment.topic = topic;
  current_assignment.pov = pov;
  double ret = 1.0;
  // Revert probability
  if (revision->parent >= 0) {
    ret *= reference_probability(mmap_info,
				 get_revision_assignment(mmap_info, revision->parent), 
				 &current_assignment, revision->disagrees,
				 index_patch);
  }
  if (revision->child >= 0 && include_child) {
    ret *= reference_probability(mmap_info, &current_assignment,
				 get_revision_assignment(mmap_info, revision->child),
				 get_revision(mmap_info, revision->child)->disagrees,
				 index_patch);
  }

  // Topic, pov probability
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  if (include_user_topic_pov) {
    int pov_correction = 0;
    if (index_patch
	&& index_patch->user == revision->user 
	&& pov == index_patch->pov
	&& topic == index_patch->topic) {
      pov_correction = 1;
    }

    double* user_pov_dist;
    get_user_topics(mmap_info, revision->user, &user_pov_dist);
    // Alpha is already added to this array. The normalizing constant is
    // independant of topic/pov, so we exclude it here.
    ret *= (user_pov_dist[topic * revision_assignment_header->pov_per_topic + pov] 
	    - pov_correction);
  }

  // Page probability
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info->topic_index_mmap;
  int64_t* page_dist;
  struct topic_summary* topic_summary;
  get_topic_summary(mmap_info, topic, &topic_summary, NULL, &page_dist);
  int64_t topic_page_revisions = page_dist[revision->article];
  int64_t topic_revisions = topic_summary->total_revisions;
  if (index_patch && index_patch->topic == topic) {
    topic_revisions -= 1;
    if (index_patch->page == revision->article) {
      topic_page_revisions -= 1;
    }
  }
  ret *= ((double)(topic_page_revisions) + revision_assignment_header->beta) 
    / ((double)(topic_revisions)
       + revision_assignment_header->beta * topic_summary_header->num_pages);
  assert(ret > 0.0);
  return ret;
}

void fill_index_patch(const struct mmap_info* mmap_info, 
		      int64_t revision_id,
		      struct index_patch* index_patch) {
  const struct revision* revision = get_revision(mmap_info, revision_id);
  index_patch->user = revision->user;
  index_patch->page = revision->article;
  const struct revision_assignment* revision_assignment 
    = get_revision_assignment(mmap_info, revision_id);
  index_patch->topic = revision_assignment->topic;
  index_patch->pov = revision_assignment->pov;
  index_patch->disagrees = revision->disagrees;
  if (revision->parent >= 0) {
    const struct revision_assignment* parent_assignment
      = get_revision_assignment(mmap_info, revision->parent);
    index_patch->parent_topic = parent_assignment->topic;
    index_patch->parent_pov = parent_assignment->pov;
  } else {
    index_patch->parent_topic = -1;
    index_patch->parent_pov = -1;
  }
  if (revision->child >= 0) {
    const struct revision_assignment* child_assignment
      = get_revision_assignment(mmap_info, revision->child);
    index_patch->child_topic = child_assignment->topic;
    index_patch->child_pov = child_assignment->pov;
    index_patch->child_disagrees = get_revision(mmap_info, 
						revision->child)->disagrees;
  } else {
    index_patch->child_topic = -1;
    index_patch->child_pov = -1;
    index_patch->child_disagrees = -1;
  }
}

double users_pages_probability_modn(const struct mmap_info* mmap_info, int sample, int modn) {
  struct revision_assignment_header* revision_assignment_header 
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  struct user_topic_header* user_topic_header = (struct user_topic_header*)mmap_info->user_topic_mmap;
  double log_likelihood = 0.0;
  int64_t user_edits;
  double* user_topic_pov_dist;
  int topic_pov_count
    = revision_assignment_header->num_topics * revision_assignment_header->pov_per_topic;
  for (int64_t user_num = sample; user_num < user_topic_header->num_users; user_num += modn) {
    get_user(mmap_info, user_num, &user_edits, NULL);
    get_user_topics(mmap_info, user_num, &user_topic_pov_dist);
    if (user_edits <= 0) {
      // This user does not actually exist
      continue;
    }
    log_likelihood -= gsl_sf_lngamma(user_edits + revision_assignment_header->alpha * topic_pov_count);
    for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
      for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
	log_likelihood += gsl_sf_lngamma(user_topic_pov_dist[topic * revision_assignment_header->pov_per_topic
							     + pov]);
      }
    }
  }
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info->topic_index_mmap;
  int64_t* page_dist;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    get_topic_summary(mmap_info, topic, NULL, NULL, &page_dist);
    for (int64_t page = sample; page < topic_summary_header->num_pages; page += modn) {
      log_likelihood += gsl_sf_lngamma(page_dist[page] + revision_assignment_header->beta);
    }
  }
  return log_likelihood;
}

double log_likelihood(const struct mmap_info* mmap_info) {
  return log_likelihood_gamma(mmap_info, 1);
}

double log_likelihood_gamma(const struct mmap_info* mmap_info, int include_users_pages) {
  struct user_topic_header* user_topic_header = (struct user_topic_header*)mmap_info->user_topic_mmap;
  struct revision_assignment_header* revision_assignment_header 
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  int topic_pov_count
    = revision_assignment_header->num_topics * revision_assignment_header->pov_per_topic;
  double log_likelihood = 0.0;
  log_likelihood += user_topic_header->num_users
    * gsl_sf_lngamma(revision_assignment_header->alpha * topic_pov_count);
  log_likelihood -= user_topic_header->num_users * topic_pov_count
    * gsl_sf_lngamma(revision_assignment_header->alpha);
  if (include_users_pages) {
    log_likelihood += users_pages_probability_modn(mmap_info, 0, 1);
  }
  
  struct topic_summary_header* topic_summary_header
    = (struct topic_summary_header*)mmap_info->topic_index_mmap;
  struct topic_summary* topic_summary;
  struct pov_summary* pov_dist;
  struct pov_summary* pov_summary;
  log_likelihood += revision_assignment_header->num_topics 
    * gsl_sf_lngamma(revision_assignment_header->beta * topic_summary_header->num_pages);
  log_likelihood -= revision_assignment_header->num_topics * topic_summary_header->num_pages
    * gsl_sf_lngamma(revision_assignment_header->beta);
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    get_topic_summary(mmap_info, topic, &topic_summary, &pov_dist, NULL);
    log_likelihood -= gsl_sf_lngamma(topic_summary->total_revisions + revision_assignment_header->beta
				     * topic_summary_header->num_pages);
    // General reverts
    log_likelihood += gsl_sf_lngamma(topic_summary->revert_general_count
				     + revision_assignment_header->gamma_alpha);
    log_likelihood += gsl_sf_lngamma(topic_summary->norevert_general_count
				     + revision_assignment_header->gamma_beta);
    log_likelihood -= gsl_sf_lngamma(topic_summary->revert_general_count
				     + topic_summary->norevert_general_count
				     + revision_assignment_header->gamma_alpha
				     + revision_assignment_header->gamma_beta);
    // Topic reverts
    log_likelihood += gsl_sf_lngamma(topic_summary->revert_topic_count
				     + revision_assignment_header->gamma_alpha);
    log_likelihood += gsl_sf_lngamma(topic_summary->norevert_topic_count
				     + revision_assignment_header->gamma_beta);
    log_likelihood -= gsl_sf_lngamma(topic_summary->revert_topic_count
				     + topic_summary->norevert_topic_count
				     + revision_assignment_header->gamma_alpha
				     + revision_assignment_header->gamma_beta);
    
    // POV reverts
    for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
      for (int ant_pov = 0; ant_pov < revision_assignment_header->pov_per_topic - 1; ++ant_pov) {
	pov_summary = pov_dist + pov * (revision_assignment_header->pov_per_topic - 1) + ant_pov;
	log_likelihood += gsl_sf_lngamma(pov_summary->revert_count
					 + revision_assignment_header->psi_alpha);
	log_likelihood += gsl_sf_lngamma(pov_summary->norevert_count
					 + revision_assignment_header->psi_beta);
	log_likelihood -= gsl_sf_lngamma(pov_summary->revert_count
					 + pov_summary->norevert_count
					 + revision_assignment_header->psi_alpha
					 + revision_assignment_header->psi_beta);
      }
    }
  }
  log_likelihood += 2 * revision_assignment_header->num_topics
    * gsl_sf_lngamma(revision_assignment_header->gamma_alpha + revision_assignment_header->gamma_beta);
  log_likelihood -= 2 * revision_assignment_header->num_topics 
    * gsl_sf_lngamma(revision_assignment_header->gamma_alpha);
  log_likelihood -= 2 * revision_assignment_header->num_topics
    * gsl_sf_lngamma(revision_assignment_header->gamma_beta);
  
  log_likelihood += revision_assignment_header->pov_per_topic 
    * (revision_assignment_header->pov_per_topic - 1) 
    * revision_assignment_header->num_topics
    * gsl_sf_lngamma(revision_assignment_header->psi_alpha + revision_assignment_header->psi_beta);
  log_likelihood -= revision_assignment_header->pov_per_topic
    * (revision_assignment_header->pov_per_topic - 1)
    * revision_assignment_header->num_topics 
    * gsl_sf_lngamma(revision_assignment_header->psi_alpha);
  log_likelihood -= revision_assignment_header->pov_per_topic
    * (revision_assignment_header->pov_per_topic - 1)
    * revision_assignment_header->num_topics
    * gsl_sf_lngamma(revision_assignment_header->psi_beta);

  return log_likelihood;
}

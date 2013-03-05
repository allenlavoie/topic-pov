#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <strings.h>

#include "comparisons.h"
#include "index.h"
#include "parse_mmaps.h"
#include "probability.h"
#include "sample.h"

void all_pov_controversy(const struct mmap_info* mmap_info,
			 double* controversy_by_pov) {
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
      controversy_by_pov[topic * revision_assignment_header->pov_per_topic + pov]
	= pov_controversy(mmap_info, topic, pov);
    }
  }
}

double average_pov_antagonism(const struct mmap_info* mmap_info, 
			      struct pov_summary* pov_summary_array,
			      int first_pov, int second_pov) {
  struct pov_summary* pov_summary;
  pov_summary = get_ant_pov(mmap_info, pov_summary_array, first_pov, second_pov);
  if (pov_summary->revert_count == 0) {
    return 0.0;
  }
  return (double)(pov_summary->revert_count)
    / (double)(pov_summary->revert_count + pov_summary->norevert_count);
}

double pov_controversy(const struct mmap_info* mmap_info, 
		       int topic, int pov) {
  struct topic_summary* topic_summary;
  struct pov_summary* pov_summary;
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  get_topic_summary(mmap_info, topic, &topic_summary, &pov_summary, NULL);
  double ret = 0.0;
  for (int other_pov = 0; other_pov < revision_assignment_header->pov_per_topic; ++other_pov) {
    if (other_pov == pov) {
      continue;
    }
    ret += average_pov_antagonism(mmap_info, pov_summary, other_pov, pov);
    ret += average_pov_antagonism(mmap_info, pov_summary, pov, other_pov);
  }
  return ret / (2.0 * (double)(revision_assignment_header->pov_per_topic - 1));
}

void init_pov_workspace(const struct mmap_info* mmap_info,
			struct pov_workspace* pov_workspace) {
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  pov_workspace->pov_edit_counts = malloc(sizeof(int64_t)
					  * revision_assignment_header->num_topics
					  * revision_assignment_header->pov_per_topic);
}

void free_pov_workspace(struct pov_workspace* pov_workspace) {
  free(pov_workspace->pov_edit_counts);
}
 
void edits_on_max_pov(const struct mmap_info* mmap_info, 
		      struct pov_workspace* pov_workspace,
		      const int64_t* revision_ids,
		      int64_t count_revisions,
		      int64_t* count_on_max, 
		      int* max_topic, int* max_pov,
		      double* entropy) {
 struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  bzero(pov_workspace->pov_edit_counts, sizeof(int64_t)
	  * revision_assignment_header->num_topics
	  * revision_assignment_header->pov_per_topic);
  for (int64_t i = 0; i < count_revisions; ++i) {
    const struct revision_assignment* revision_assignment
      = get_revision_assignment(mmap_info, revision_ids[i]);
    pov_workspace->pov_edit_counts[revision_assignment->topic
				   * revision_assignment_header->pov_per_topic
				   + revision_assignment->pov] += 1;
  }
  *count_on_max = 0;
  *entropy = 0.0;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    for (int pov = 0; pov < revision_assignment_header->pov_per_topic; ++pov) {
      int64_t current_count
	= pov_workspace->pov_edit_counts[topic * revision_assignment_header->pov_per_topic + pov];
      double count_fraction = (double)current_count / (double)count_revisions;
      if (count_fraction > 0.0) {
	*entropy -= count_fraction * log(count_fraction);
      }
      if (current_count > *count_on_max) {
	*count_on_max = current_count;
	*max_topic = topic;
	*max_pov = pov;
      }
    }
  }
}

double user_antagonism(const struct mmap_info* mmap_info,
		       int first_user, int second_user) {
  int64_t first_user_revisions;
  int64_t second_user_revisions;
  double* first_user_dist;
  double* second_user_dist;
  get_user(mmap_info, first_user, &first_user_revisions, NULL);
  get_user(mmap_info, second_user, &second_user_revisions, NULL);
  get_user_topics(mmap_info, first_user, &first_user_dist);
  get_user_topics(mmap_info, second_user, &second_user_dist);
  struct topic_summary* topic_summary;
  struct pov_summary* first_pov_summary;
  struct pov_summary* second_pov_summary;
  struct pov_summary* pov_summary;
  struct revision_assignment_header* revision_assignment_header
    = (struct revision_assignment_header*)mmap_info->revision_assignment_mmap;
  double total_probability = 0.0;
  for (int topic = 0; topic < revision_assignment_header->num_topics; ++topic) {
    for (int first_pov = 0; first_pov < revision_assignment_header->pov_per_topic; ++first_pov) {
      for (int second_pov = 0; second_pov < revision_assignment_header->pov_per_topic; ++second_pov) {
	if (first_pov == second_pov) {
	  continue;
	}
	get_topic_summary(mmap_info, topic, &topic_summary, &pov_summary, NULL);
	first_pov_summary = get_ant_pov(mmap_info, pov_summary, first_pov, second_pov);
	second_pov_summary = get_ant_pov(mmap_info, pov_summary, second_pov, first_pov);
	total_probability +=
	  first_user_dist[topic * revision_assignment_header->pov_per_topic + first_pov]
	  / (first_user_revisions
	     + revision_assignment_header->num_topics
	     * revision_assignment_header->pov_per_topic
	     * revision_assignment_header->alpha)
	  * second_user_dist[topic * revision_assignment_header->pov_per_topic + second_pov]
	  / (second_user_revisions
	     + revision_assignment_header->num_topics
	     * revision_assignment_header->pov_per_topic
	     * revision_assignment_header->alpha)
	  * (0.5 * (first_pov_summary->revert_count + revision_assignment_header->psi_alpha)
	     / (first_pov_summary->revert_count + revision_assignment_header->psi_alpha
		+ first_pov_summary->norevert_count + revision_assignment_header->psi_beta)
	     + 0.5 * (second_pov_summary->revert_count + revision_assignment_header->psi_alpha)
	     / (second_pov_summary->revert_count + revision_assignment_header->psi_alpha
		+ second_pov_summary->norevert_count + revision_assignment_header->psi_beta));
      }
    }
  }
  return total_probability;
}

void count_pov_reverts(const struct mmap_info* mmap_info,
		       const int64_t* revision_ids,
		       int64_t count_revisions,
		       int64_t* num_pov_reverts,
		       int64_t* num_pov_reverted,
		       int64_t* num_reverts) {
  *num_reverts = 0;
  *num_pov_reverts = 0;
  *num_pov_reverted = 0;
  for (int64_t i = 0; i < count_revisions; ++i) {
    const struct revision* revision = get_revision(mmap_info, revision_ids[i]);
    const struct revision_assignment* revision_assignment
      = get_revision_assignment(mmap_info, revision_ids[i]);
    if (revision->disagrees) {
      ++(*num_reverts);
      if (revision->parent >= 0) {
	const struct revision_assignment* parent_assignment
	  = get_revision_assignment(mmap_info, revision->parent);
	if (parent_assignment->topic == revision_assignment->topic
	    && parent_assignment->pov != revision_assignment->pov) {
	  ++(*num_pov_reverts);
	}
      }
      if (revision->child >= 0) {
	const struct revision* child = get_revision(mmap_info, revision->child);
	if (child->disagrees) {
	  const struct revision_assignment* child_assignment
	    = get_revision_assignment(mmap_info, revision->child);
	  if (child_assignment->topic == revision_assignment->topic
	      && child_assignment->pov != revision_assignment->pov) {
	    ++(*num_pov_reverted);
	  }
	}
      }
    }
  }
}

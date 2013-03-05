/* Utility functions for computing statistics about users, pages, and
   pairs of users. */

#ifndef __COMPARISONS_H__
#define __COMPARISONS_H__

struct mmap_info;

struct pov_workspace {
  int64_t* pov_edit_counts;
};

/* Compute the level of controversy for this (topic, POV), which is
   defined as the observed fraction of interactions between other POVs
   on this topic which result in a disagreement. */
double pov_controversy(const struct mmap_info* mmap_info, 
		       int topic, int pov);
/* Compute the level of controversy for each (topic, POV), storing the
   result in controversy_by_pov, which must have size at least
   num_topics * povs_per_topic. */
void all_pov_controversy(const struct mmap_info* mmap_info,
			 double* controversy_by_pov);

/* Compute the probability of two users having a POV disagreement
   given that their edits are next to each other on the same page. */
double user_antagonism(const struct mmap_info* mmap_info,
		       int first_user, int second_user);

/* Initialize or free a struct which contains allocated memory used
   when computing page and user statistics. */
void init_pov_workspace(const struct mmap_info* mmap_info,
			struct pov_workspace* pov_workspace);
void free_pov_workspace(struct pov_workspace* pov_workspace);

/* Over the count_revisions revisions specified by the revision_ids
   array, compute the number of revisions on the max POV (stored in
   the variable pointed to by count_on_max), the (topic, POV) that
   occurs most frequently (max_topic, max_pov), and the topic/POV
   entropy (entropy).

   Usually, the revisions specified by revision_ids and
   count_revisions will be all of the revisions by a user, or all the
   revisions on a page.*/
void edits_on_max_pov(const struct mmap_info* mmap_info, 
		      struct pov_workspace* pov_workspace,
		      const int64_t* revision_ids,
		      int64_t count_revisions,
		      int64_t* count_on_max, 
		      int* max_topic, int* max_pov,
		      double* entropy);

/* Over the count_revisions revisions specified by the revision_ids
   array, count the total number of reverts (num_reverts), the POV
   reverts (edits in this set which revert another edit;
   num_pov_reverts), and the number of edits in this set which are
   themselves POV reverted (num_pov_reverted). */
void count_pov_reverts(const struct mmap_info* mmap_info,
		       const int64_t* revision_ids,
		       int64_t count_revisions,
		       int64_t* num_pov_reverts,
		       int64_t* num_pov_reverted,
		       int64_t* num_reverts);
#endif

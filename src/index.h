/* Mmap formats for storing revisions and indexing them, and indexing
   revision assignments. */

#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdint.h>

/* Revision data indexes */

struct revision_header {
  int64_t count_revisions;
};

struct revision {
  int32_t article;
  int64_t timestamp;
  int32_t user;
  int64_t parent;
  int64_t child;
  int32_t disagrees;
};

struct page_header {
  int64_t count_pages;
};

struct user_header {
  int64_t count_users;
};

struct page_index {
  int64_t count_revisions;
  // Offset in this file of an array of revision IDs (int64_t)
  int64_t revisions_offset; 
};

struct user_index {
  int64_t count_revisions;
  // Offset in this file of an array of revision IDs (int64_t)
  int64_t revisions_offset;
};

/* Topic/POV assignment indexes */

struct revision_assignment_header {
  int32_t num_topics;
  int32_t pov_per_topic;
  int64_t total_iterations;
  int64_t count_revisions;
  double psi_alpha; // Antagonistic POV
  double psi_beta;
  double gamma_alpha; // Action
  double gamma_beta;
  double beta; // Page
  double alpha; // Topic
};

struct revision_assignment {
  int32_t topic;
  int32_t pov;
};

struct topic_summary_header {
  int64_t _dummy_var;
  int32_t num_topics;
  int32_t pov_per_topic;
  int64_t num_pages;
};

struct topic_summary {
  int64_t total_revisions;
  int64_t revert_general_count;
  int64_t norevert_general_count;
  int64_t revert_topic_count;
  int64_t norevert_topic_count;
  // Followed by page and POV distributions
};

struct pov_summary {
  int64_t revert_count;
  int64_t norevert_count;
};

struct user_topic_header {
  int64_t num_users;
  int64_t num_topics;
  int64_t pov_per_topic;
  // Followed by topic/POV distribution for each user
};

#endif

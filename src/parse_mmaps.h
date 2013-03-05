/* Functions for opening, closing, and reading from memory maps. */

#ifndef __PARSE_MMAPS_H__
#define __PARSE_MMAPS_H__

#include <stdint.h>

/* Standard mmap names (within the mmap_dir). Defined in
   parse_mmaps.c. */
const char* USER_INDEX_MMAP_NAME;
const char* PAGE_INDEX_MMAP_NAME;
const char* REVISIONS_MMAP_NAME;
const char* REVISION_ASSIGNMENT_MMAP_NAME;
const char* TOPIC_INDEX_MMAP_NAME;
const char* USER_TOPIC_MMAP_NAME;

/* Keeps track of open mmaps, and how they were opened (read/write
   mmap, read-only mmap, read into memory) */
struct mmap_info {
  const char* revision_mmap;
  int64_t revision_mmap_size;
  const char* user_mmap;
  int64_t user_mmap_size;
  const char* page_mmap;
  int64_t page_mmap_size;

  char* revision_assignment_mmap;
  int64_t revision_assignment_mmap_size;
  char* topic_index_mmap;
  int64_t topic_index_mmap_size;
  char* topic_index_pages_mmap;
  char* user_topic_mmap;
  int64_t user_topic_mmap_size;

  char* revisions_mmap_name;
  char* user_mmap_name;
  char* page_mmap_name;
  char* revision_assignment_mmap_name;
  char* topic_index_mmap_name;
  char* user_topic_mmap_name;

  int rw_mmaps_inmem;
};

struct revision;
struct revision_assignment;
struct topic_summary;
struct pov_summary;

/* Functions for opening/closing mmaps. */

/* Open the standard mmaps (those that exist in the specified
   directory).

     readonly: Open them as a memory map, but mark it as private and
     do not commit changes back to disk.

     memory: Read the files into memory, then write all of the changes
     back to disk when close_mmaps is called. Nothing is written in
     between. Useful in cases where the operating system adds overhead
     to memory mapped writes, but requires all of the indexes to fit
     into memory.
     
     mmap: Use a normal read/write memory map. Changes are committed
     as soon as the operating system feels like it.  */
struct mmap_info open_mmaps_readonly(const char* directory);
struct mmap_info open_mmaps_memory(const char* directory);
struct mmap_info open_mmaps_mmap(const char* directory);

/* Unmap memory, close files. If files were opened with
   open_mmaps_memory, this commits changes back to disk. */
void close_mmaps(struct mmap_info);

/* Information access functions. Unless otherwise noted, ownership to
   memory is retained by the memory map (don't de-allocate it). */

/* Get non-assignment information about a single revision (parent,
   timestamp, user, etc.). The returned struct pointer is to part of
   the mmap, and the mmap retains ownership. */
const struct revision* get_revision(const struct mmap_info* mmap_info, int64_t revision_id);

/* Get information about a page. The number of revisions on the page
   is stored in count_revisions, and a pointer into the memory map to
   a list of revision IDs on the page is stored in revision_ids. Both
   must be non-NULL.*/
void get_page(const struct mmap_info* mmap_info, int64_t page_id,
	      int64_t* count_revisions, 
	      const int64_t** revision_ids);

/* Get information about a user. Analogous to get_page, but for a
   user. */
void get_user(const struct mmap_info* mmap_info, int64_t user_id, int64_t* count_revisions, 
	      const int64_t** revision_ids);

/* Get a pointer the topic/POV distribution (a num_topic * num_pov
   array) for this user. The pointer is stored in the pointer pointed
   to by topic_pov_dist, which then points into the memory map. The
   memory map maintains ownership of the memory thus pointed to. */
void get_user_topics(const struct mmap_info* mmap_info, int64_t user_id, double** topic_pov_dist);

/* Get information about a topic: the topic summary (see index.h), the
   point of view distribution, and the page distribution (an array of
   size num_pages, containing the number of edits on this topic on a
   given page). pov_dist should be accessed through get_ant_pov
   below. */
void get_topic_summary(const struct mmap_info* mmap_info, int32_t topic_id,
		       struct topic_summary** topic_summary,
		       struct pov_summary** pov_dist,
		       int64_t** page_dist);

/* Get the relationship between two points of view on the same
   topic. pov_dist should come from get_topic_summary. */
struct pov_summary* get_ant_pov(const struct mmap_info* mmap_info, 
				struct pov_summary* pov_dist, int32_t pov, int32_t ant_pov);

/* Get the (topic, POV) assignment for the specified revision. */
struct revision_assignment* get_revision_assignment(const struct mmap_info* mmap_info, 
						    int64_t revision_id);

/* Get a pointer to an array of all the revision assignments, along
   with the total number of revisions (stored in count_revisions). */
void get_revision_assignment_array(const struct mmap_info* mmap_info, int64_t* count_revisions, 
				   struct revision_assignment** revisions);

/* Index update functions */

/* Set the user topic/POV dist entries to alpha for all users such
   that userid % modn == sample. Useful for initializing user
   distributions in parallel. */
void initialize_user_topics(const struct mmap_info* mmap_info, int sample, int modn);

// Change the revision's assignments, updating indexes automatically.
void change_revision_assignment(const struct mmap_info* mmap_info, int64_t revision,
				int32_t new_topic, int32_t new_pov);

// Manually change indexes for revision_id by the amount specified.
void change_indexes(const struct mmap_info* mmap_info, int64_t revision_id, int32_t change_by);

/* Miscellaneous utility functions related to data storage */

/* Allocate memory for and copy revision assignments into an array,
   then return it. The caller takes ownership of the allocated
   memory. */
struct revision_assignment* copy_revision_assignments(const struct mmap_info* mmap_info);

/* Concatinate the directory and file name, adding a slash if
   necessary. The caller takes ownership of the returned memory. */
char* full_path(const char* directory, const char* file);

/* Utility functions for reading/writing full files */
char* read_file(const char* file_name, int64_t* length);
void write_file(const char* file_name, const char* to_write, int64_t length);

/* Open a file as a private memory map, not committing anything back
   to disk. */
char* open_mmap_read(const char* file_name, int64_t* mmap_size);

/* Create a file and open it as a memory map initialized to all
   zeros. Destroys any data that may have existed in the file. */
char *create_mmap(const char *file_name, int64_t length);

// Compute the size of mmap files
int64_t revision_assignment_mmap_size(int64_t count_revisions);
int64_t topic_summary_mmap_size(int32_t num_topics, int32_t pov_per_topic, int64_t num_pages);
int64_t user_topic_mmap_size(int64_t num_users, int32_t num_topics, int32_t pov_per_topic);

/* Create empty assignment indexes (not data mmaps) with the given
   parameters. Requires that data storage mmaps (revision, user index,
   page index) have already been created. */
void create_indexes(const char* directory,
		    double psi_alpha,
		    double psi_beta,
		    double gamma_alpha,
		    double gamma_beta,
		    double beta,
		    double alpha,
		    int num_topics,
		    int pov_per_topic,
		    int num_threads);

#endif

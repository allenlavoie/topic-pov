CFLAGS = --std=c99 -march=native -fmodulo-sched -fmodulo-sched-allow-regmoves -ffast-math -O3 -Wall -D_GNU_SOURCE 
#CFLAGS = --std=c99 -g -Wall -D_GNU_SOURCE 
LIBS = -lm -lgsl -lgslcblas -pthread
COMMON_OBJS = parse_mmaps.o probability.o sample.o comparisons.o
OUTDIR = ../bin

all: make_mmap verify_mmap initialize inference readout set_assignments compare_users basicstats word_probability page_user_stats check_indexes
verify_mmap: $(COMMON_OBJS) verify_mmaps.o
	gcc $(CFLAGS) $(COMMON_OBJS) verify_mmaps.o $(LIBS) -o $(OUTDIR)/verify_mmaps
make_mmap: $(COMMON_OBJS) store_revisions.o
	gcc $(CFLAGS) $(COMMON_OBJS) store_revisions.o $(LIBS) -o $(OUTDIR)/store_revisions
initialize: $(COMMON_OBJS) initialize.o
	gcc $(CFLAGS) $(COMMON_OBJS) initialize.o $(LIBS) -o $(OUTDIR)/initialize
inference: $(COMMON_OBJS) inference.o
	gcc $(CFLAGS) $(COMMON_OBJS) inference.o $(LIBS) -o $(OUTDIR)/inference
readout: $(COMMON_OBJS) readout.o
	gcc $(CFLAGS) $(COMMON_OBJS) readout.o $(LIBS) -o $(OUTDIR)/readout
set_assignments: $(COMMON_OBJS) set_assignments.o
	gcc $(CFLAGS) $(COMMON_OBJS) set_assignments.o $(LIBS) -o $(OUTDIR)/set_assignments
compare_users: $(COMMON_OBJS) compare_users.o
	gcc $(CFLAGS) $(COMMON_OBJS) compare_users.o $(LIBS) -o $(OUTDIR)/compare_users
basicstats: $(COMMON_OBJS) basicstats.o
	gcc $(CFLAGS) $(COMMON_OBJS) basicstats.o $(LIBS) -o $(OUTDIR)/basicstats
word_probability: $(COMMON_OBJS) word_probability.o
	gcc $(CFLAGS) $(COMMON_OBJS) word_probability.o $(LIBS) -o $(OUTDIR)/word_probability
page_user_stats: $(COMMON_OBJS) page_user_stats.o
	gcc $(CFLAGS) $(COMMON_OBJS) page_user_stats.o $(LIBS) -o $(OUTDIR)/page_user_stats
check_indexes: $(COMMON_OBJS) check_indexes.o
	gcc $(CFLAGS) $(COMMON_OBJS) check_indexes.o $(LIBS) -o $(OUTDIR)/check_indexes
clean:
	rm *.o
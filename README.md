topic-pov
=========

Code for fitting a topic and point of view (POV) model of collective
intelligence processes, using parallel Gibbs sampling. Also includes
synthetic data generation for testing and evaluation.

To compile and run:
```bash
make
bash test_synth.sh 50 500
```

You will need the GNU Scientific Library (GSL) headers installed to
compile everything. See comments in test_synth.sh for a high-level
overview of the inference process.

Files associated with executables (descriptions at the top of each file):

- basicstats.c
- check_indexes.c
- compare_users.c
- inference.c
- initialize.c
- page_user_stats.c
- readout.c
- set_assignments.c
- store_revisions.c
- verify_mmaps.c
- word_probability.c

Shared functions, struct definitions (descriptions in header files):

- index.h
- comparisons.h / comparisons.c
- parse_mmaps.h / parse_mmaps.c
- probability.h / probability.c
- sample.h / sample.c

Python helper scripts (in bin/):

- compare_clusterings.py
- synth.py
- topic_pov_to_clusters.py

Bash to glue everything together:

- test_synth.sh
- initialize.sh

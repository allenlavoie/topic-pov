TOPICS=$1
POV=4
THREADS=8
USERS=$2
MMAP_DIR="synth_mmaps"
# Generate synthetic data according to the model, store it in synth_data.txt
python bin/synth.py $TOPICS $POV $USERS > synth_data.txt
mkdir -p $MMAP_DIR
# Record the text-formatted data in memory maps
bin/store_revisions $MMAP_DIR synth_data.txt
# (Optional) check to make sure the memory maps match the text data
bin/verify_mmaps $MMAP_DIR synth_data.txt
# Initialize topic and POV assignments (hyper-parameters are listed in
# initialize.sh, and should match those in synth.py)
bin/initialize.sh $MMAP_DIR $TOPICS $POV $THREADS
# Perform inference for 100 iterations (log likelihood written to synth_log.txt)
bin/inference $MMAP_DIR 100 $THREADS >> synth_log.txt
# Write the assignments to a text format
bin/readout $MMAP_DIR 10 $THREADS > inferred_topic_pov.txt 2>>synth_log.txt
# Convert the "revision_id topic pov" format to (topic, POV) clusters
python bin/topic_pov_to_clusters.py inferred_topic_pov.txt > inferred_clusters.txt
# Compare the inferred clustering to the true clustering from synth.py
python bin/compare_clusterings.py inferred_clusters.txt synthetic_clustering.txt $POV

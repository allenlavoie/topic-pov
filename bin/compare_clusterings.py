"""Compare inferred (topic, POV) clusters to a true clustering."""

from sklearn import metrics
import sys
import random

POV_PER_CLUSTER = int(sys.argv[3])
TOPICS_ONLY = False

def read_clustering_file(fname, topics_only=False, randomize_pov=False):
    assert (not topics_only) or (not randomize_pov)
    f = open(fname, 'rb')
    labels = {}
    for cluster_num, line in enumerate(f):
        for item in line.split():
            if topics_only:
                labels[int(item)] = cluster_num // POV_PER_CLUSTER
            elif randomize_pov:
                labels[int(item)] = (cluster_num - cluster_num % POV_PER_CLUSTER
                                     + random.choice(range(POV_PER_CLUSTER)))
            else:
                labels[int(item)] = cluster_num
    return zip(*sorted(labels.items()))[1]

def rand_compare(truth_file, estimate_file, topics_only=False, randomize_pov=False):
    truth_labels = read_clustering_file(truth_file,
                                        topics_only=topics_only)
    estimate_labels = read_clustering_file(estimate_file,
                                           topics_only=topics_only,
                                           randomize_pov=randomize_pov)
    return metrics.adjusted_rand_score(truth_labels, estimate_labels)

def main():
    first_clustering_file = sys.argv[1]
    second_clustering_file = sys.argv[2]
    print rand_compare(second_clustering_file, first_clustering_file),\
        rand_compare(second_clustering_file, first_clustering_file, topics_only=True),\
        rand_compare(second_clustering_file, first_clustering_file, randomize_pov=True)

if __name__ == '__main__':
    main()

"""Convert (topic, POV) assignments in the form:
revision_id topic pov
To "clusters" of revision_ids, one per (topic, POV)."""
import sys

def get_clusters(revisions_it):
    clusters = {}
    for line in revisions_it:
        revision, topic, pov = [int(a) for a in line.split()]
        clusters.setdefault(topic, {}).setdefault(pov, []).append(revision)
    for povs in clusters.itervalues():
        for revisionlist in povs.itervalues():
            yield revisionlist
        

def main():
    f = open(sys.argv[1], 'rb')
    for povs in get_clusters(f):
        print '%s' % (' '.join([str(a) for a in povs]))        



if __name__ == '__main__':
    main()

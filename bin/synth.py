"""Generate data according to the model, and record the true (topic,
POV) assignments for performance evaluation."""

from numpy.random.mtrand import dirichlet
from numpy.random import beta, poisson, multinomial, uniform
import numpy
import random
import sys

TOPICS = int(sys.argv[1])
POV_PER_TOPIC = int(sys.argv[2])
PAGES_PER_USER = 9784794.0 / 31604659.0
USERS = int(sys.argv[3])
PAGES = int(USERS * PAGES_PER_USER)

PSI_ALPHA = 0.8
PSI_BETA = 0.2
GAMMA_ALPHA = 0.2
GAMMA_BETA = 0.8
BETA = 0.1
ALPHA = 5.0 / float(TOPICS * POV_PER_TOPIC)

EDITS_PER_USER_PARETO_ALPHA = 0.8 # About 35 revs/user
#EDITS_PER_USER_PARETO_ALPHA = 0.5836 # About 170 revs/user
EDITS_PER_USER_MAX = 147696
#EDITS_PER_USER_MAX = 103266
EDITS_PER_USER_MIN = 1

def generate_pareto(alpha, lower, upper):
    alpha = float(alpha)
    lower = float(lower)
    upper = float(upper)
    uniform = random.random()
    return (-(uniform * upper ** alpha - uniform * lower ** alpha - upper ** alpha)
             / (upper ** alpha * lower ** alpha)) ** -(1.0 / alpha)

def main():
    user_topic_preferences = dirichlet([ALPHA] * TOPICS * POV_PER_TOPIC, USERS)
    topic_page_dists = dirichlet([BETA] * PAGES, TOPICS)
    revert_general = beta(GAMMA_ALPHA, GAMMA_BETA, TOPICS)
    revert_topic = beta(GAMMA_ALPHA, GAMMA_BETA, TOPICS)
    revert_pov = beta(PSI_ALPHA, PSI_BETA, TOPICS * POV_PER_TOPIC * (POV_PER_TOPIC - 1))
    edits_per_user = [generate_pareto(EDITS_PER_USER_PARETO_ALPHA,
                                      EDITS_PER_USER_MIN,
                                      EDITS_PER_USER_MAX) for i in range(USERS)]

    edits_by_page = {}
    for user_num in range(USERS):
        topic_edit_counts = multinomial(edits_per_user[user_num],
                                        user_topic_preferences[user_num])
        for topic_pov_num, topic_count in enumerate(topic_edit_counts):
            pov_num = topic_pov_num % POV_PER_TOPIC
            topic_num = (topic_pov_num - pov_num) // POV_PER_TOPIC
            if topic_count == 0:
                continue
            topic_page_edit_count = multinomial(topic_count,
                                                topic_page_dists[topic_num])
            pages = [[page_num] * count
                     for page_num, count
                     in enumerate(topic_page_edit_count)
                     if count > 0]
            pages = [item for sublist in pages for item in sublist]
            for page in pages:
                edits_by_page.setdefault(page, []).append((topic_num, pov_num, user_num))
    for edits in edits_by_page.itervalues():
        random.shuffle(edits)
    current_revision = 1
    flat_edits = []
    for page_num in range(PAGES):
        if page_num not in edits_by_page:
            continue
        for edit_number, (current_topic, current_pov, current_user) in enumerate(
            edits_by_page[page_num]):
            flat_edits.append((current_revision, current_topic, current_pov))
            if edit_number == 0:
                print page_num + 1, 1234, current_user + 1, current_revision, -1, 'f'
                current_revision += 1
                continue
            (previous_topic, previous_pov, previous_user) = edits_by_page[
                page_num][edit_number - 1]
            revert = False
            if current_topic == previous_topic:
                if current_pov != previous_pov:
                    if previous_pov > current_pov:
                        renumbered_previous_pov = previous_pov - 1
                    else:
                        renumbered_previous_pov = previous_pov
                    if uniform() < revert_pov[
                        current_topic * POV_PER_TOPIC * (POV_PER_TOPIC - 1)
                        + (POV_PER_TOPIC - 1) * current_pov
                        + renumbered_previous_pov]:
                        revert = True
                else:
                    if uniform() < revert_topic[current_topic]:
                        revert = True
            else:
                if uniform() < revert_general[current_topic]:
                    revert = True
            if revert:
                revert_string = 't'
            else:
                revert_string = 'f'
            print page_num + 1, 1234, current_user + 1, current_revision,\
                current_revision - 1, revert_string
            current_revision += 1
    topicpovout = open('synthetic_topics_povs.txt', 'w')
    clustering = {}
    for revision, topic, pov in flat_edits:
        clustering.setdefault(topic, {}).setdefault(pov, []).append(revision)
        topicpovout.write('%d %d %d\n' % (revision, topic, pov))
    clusteringout = open('synthetic_clustering.txt', 'w')
    for povs in clustering.itervalues():
        for revisionlist in povs.itervalues():
            clusteringout.write('%s\n' % (' '.join([str(a) for a in revisionlist])))

    users_by_cluster = {}
    userclusteringout = open('synthetic_user_clustering.txt', 'w')
    for user_num in range(USERS):
        prefs = user_topic_preferences[user_num]
        max_povtopic = max(enumerate(prefs), key=lambda x:x[1])[0]
        max_pov = max_povtopic % POV_PER_TOPIC
        max_topic = (max_povtopic - max_pov) // POV_PER_TOPIC
        users_by_cluster.setdefault((max_topic, max_pov), []).append(user_num + 1)
    for (topic, pov), users in users_by_cluster.iteritems():
        userclusteringout.write('%s\n' % (' '.join([str(a) for a in users])))
        
                
if __name__ == '__main__':
    main()

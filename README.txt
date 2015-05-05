    The usage of clustering2-whole.py:

    python clustering2-whole.py tweets.txt plain head plain_head source desc 
    author_cluster output_tweets tokens length

    The usage of processtweets.py:

    python processtweets.py present_head previous_head 1 keyword_1 keyword_2 
    tweet_events


    after running the parseraw.py,
    we run the final_output.py to generate the event_tweetcontent file.
    Then we run the ease_labeling_summary.py to generate the summay.
    Note that ease_labeling_summary.py should be fed with the directory that the 
    final_output.py generates (i.e. data_dir/dist_temp)

Then we run the gen_bipartite_graph.py to generate the nodes and edges for the 
bipartite graph.

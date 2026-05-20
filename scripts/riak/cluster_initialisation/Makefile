

# This will join the cluster and create the baseline bucket
cluster_setup:
	./1_riak_base_setup.sh

# This will pre-warm the data that is needed
pre_warm_everything:
	../ryw/1_riak_prewarm_counters.py
	../fanout/1_prewarm.py

# This calls both in order
setup_all: cluster_setup pre_warm_everything
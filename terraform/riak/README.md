# Helper Commands for Riak


## HEALTH

sudo -u riak /opt/openriak/bin/riak ping
sudo -u riak /opt/openriak/bin/riak-admin member-status
sudo ss -lntp | egrep ':(8098|8087)\b'


## SEED Cluster Status
sudo -u riak /opt/openriak/bin/riak-admin member-status
sudo -u riak /opt/openriak/bin/riak-admin cluster status

## SEED Plan / Commit
sudo -u riak /opt/openriak/bin/riak-admin cluster plan
sudo -u riak /opt/openriak/bin/riak-admin cluster commit

## NODE JOIN
SEED=172.31.2.146 <- Obviously reset this
sudo -u riak /opt/openriak/bin/riak-admin cluster join "riak@${SEED}"
sudo -u riak /opt/openriak/bin/riak-admin member-status


## WATCH CONVERGENCE FROM SEED
watch -n 2 "sudo -u riak /opt/openriak/bin/riak-admin member-status; echo; sudo -u riak /opt/openriak/bin/riak-admin transfers"



## SEED Ring Status
sudo -u riak /opt/openriak/bin/riak-admin ring-status
sudo -u riak /opt/openriak/bin/riak-admin transfers
sudo -u riak /opt/openriak/bin/riak-admin handoff


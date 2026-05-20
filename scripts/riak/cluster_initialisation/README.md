# Cluster Initialisation & Helper Commands

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

---

# Bucket Creation

## This creates a bucket 
- Defines a bucket type called counters 
- Sets its datatype to counter (CRDT)
``` 
/opt/openriak/bin/riak-admin bucket-type create counters '{"props":{"datatype":"counter"}}'
```

## This enables the bucket
- The bucket type is usable cluster-wide 
- You can now read/write CRDT counters into it

```
/opt/openriak/bin/riak-admin bucket-type activate counters
 ```

## Verify the bucket type 
```
/opt/openriak/bin/riak-admin bucket-type status counters
```


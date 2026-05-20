# Fault Injection Commands
Note: These should always be run on node-3. There should only be one active fault at a time. 

## Inject Latency
This adds latency:
`sudo tc qdisc add dev ens5 root netem delay 100ms 30ms distribution normal`

This removes latency:
`sudo tc qdisc del dev ens5 root && tc qdisc show dev ens5`


## Inject Packetloss
This adds packet loss:
`sudo tc qdisc add dev ens5 root netem loss 1% 25%`

This removes packet loss:
`sudo tc qdisc del dev ens5 root && tc qdisc show dev ens5`
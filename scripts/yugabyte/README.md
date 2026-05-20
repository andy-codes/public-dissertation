
## SCP the script file to the target server
`scp -i key.pem yb_schema.sql ec2-user@52.210.104.115:~`

## Connect to Yugabyte and run the schema setup
`ssh ec2-user@52.210.104.115`

## Execute the schema file
`ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d yugabyte -f schema.sql`


## Verify table exists
`ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d fanoutdb -c "\d yb_counters"`


## Insert command
```
ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d fanoutdb -c \
"INSERT INTO yb_counters(k, v) VALUES (1, 0) ON CONFLICT (k) DO NOTHING; SELECT * FROM yb_counters WHERE k = 1;"
```


## Useful for EC2 instances that want to connect
```
host=127.0.0.1
port=5433
user=yugabyte
database=fanoutdb
```
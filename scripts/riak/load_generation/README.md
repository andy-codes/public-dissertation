# How to launch locust

Substitute filename of course

```
locust -f locustfile.py
```

Access the web client
`http://localhost:8089`

# Run it headless

Headless command:
```
locust -f locust.py --headless \
  --users 100 --spawn-rate 20 \
  --run-time 60m \
  --csv runs/bg_load_run1/locust_bg
```


This should produce:

~90% POST increments

~10% GETs


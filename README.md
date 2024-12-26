# byomod

## Run exporter
After cloning the repo:

```bash
cd byomod
echo "export PYTHONPATH=$PYTHONPATH:{$PWD}/src" > .env
pipenv install
```

If you use Visual Code, you can use the 'firehose_exporter' launch definition
to run the exporter. Alternatively, you can use

```bash
cd src
pipenv run python3 -m uvicorn firehose_exporter:APP --host 0.0.0.0 --port 8000 --proxy-headers
```

## Use with Prometheus and Grafana

Use the following SD_FILE_CONFIG file to make Prometheus scrape the exporter:

```yaml
- targets:
  - <ip address of the server running the firehose_exporter>:8000
  labels:
    job: bsky
    service: bsky
```

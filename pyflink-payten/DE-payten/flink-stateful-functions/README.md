# Stateful example

Run it with:

```bash
docker-compose up --build
```

Saving the job:
```bash
curl -X POST localhost:8081/jobs/[JOB_ID_FROM_DASHBOARD]/savepoints -d '{"cancel-job": false}'
```

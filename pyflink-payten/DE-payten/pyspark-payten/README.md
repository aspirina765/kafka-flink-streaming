# Pyspark

Starting container with:

```bash
docker-compose up -d --build
```

Running the job by entering inside the container:
```bash
docker-compose exec spark-worker-1 bash
```

Then, start the job:
```bash
/spark/bin/spark-submit /app/queries/1-word_count.py
```

For streaming job, we need additional Kafka package:
```bash
/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /app/quieries/9-window_stream.py

```
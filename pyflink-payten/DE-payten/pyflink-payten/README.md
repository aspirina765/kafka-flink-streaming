# Solver Pyflink
To provide a quick-start environment and examples for users to quickly understand the features of PyFlink

# Environment Setup

1. Install [Docker](https://www.docker.com).
2. Get Docker Compose configuration
```
git clone https://github.com/ThingsSolver/pyflink-payten.git
```
3. Setup environment
* **Linux & MacOS**

```bash
cd DE-payten/pyflink-payten/pyflink-payten
docker-compose up -d
```

* **Windows**

```
cd pyflink-payten/pyflink-payten
set COMPOSE_CONVERT_WINDOWS_PATHS = 1
docker-compose up -d
```

You can check whether the environment is running correctly
by visiting Flink Web UI:

http://localhost:8088

Run test job:

```bash
docker-compose exec jobmanager . /bin/flink run -py /opt/examples/queries/1-word_count.py
```

Input data folder should be:

```bash
/pyflink-payten/examples/data/input
```

Output data folder should be:

```bash
/pyflink-payten/examples/data/output
```
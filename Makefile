.PHONY : help up down download etl load benchmark clean

# Load .env if it exists
-include .env
export

SPARK_MASTER_CONTAINER = spark-master
CLICKHOUSE_CONTAINER   = clickhouse
CH_CLIENT              = docker exec $(CLICKHOUSE_CONTAINER) clickhouse-client --max_threads 2 --max_memory_usage 1500000000
SPARK_SUBMIT           = docker exec $(SPARK_MASTER_CONTAINER) \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 768m \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.executor.instances=1 \
    --conf spark.sql.files.maxPartitionBytes=4m \
    --conf spark.sql.autoBroadcastJoinThreshold=128m \
    --conf spark.sql.shuffle.partitions=64

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	    awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ───────────────────────────────────────────────────────────

up: ## Start Spark cluster + ClickHouse
	docker compose up -d
	@echo "Waiting for ClickHouse to be healthy..."
	@until docker compose exec clickhouse wget -q --spider http://localhost:8123/ping 2>/dev/null; do \
	    sleep 2; \
	done
	@echo "ClickHouse is ready."
	@echo "Spark UI  → http://localhost:8085"
	@echo "Worker UI → http://localhost:18081"
	@echo "ClickHouse→ http://localhost:8123"

down: ## Stop and remove containers
	docker compose down

# ── Pipeline Steps ───────────────────────────────────────────────────────────

download: ## Download IMDb dataset via kagglehub (auto-authenticates with Kaggle)
	python3 download_dataset.py

etl: ## Run PySpark ETL job (produces Snappy Parquet in data/lake/)
	rm -rf data/lake/titles data/lake/people data/lake/principals data/lake/akas
	$(SPARK_SUBMIT) /opt/spark/jobs/etl_job.py --input /data/raw --output /data/lake --dataset titles
	$(SPARK_SUBMIT) /opt/spark/jobs/etl_job.py --input /data/raw --output /data/lake --dataset people
	$(SPARK_SUBMIT) /opt/spark/jobs/etl_job.py --input /data/raw --output /data/lake --dataset principals
	$(SPARK_SUBMIT) /opt/spark/jobs/etl_job.py --input /data/raw --output /data/lake --dataset akas
	@echo "ETL complete. Parquet files written to data/lake/"

load: ## Load all 4 Parquet datasets into ClickHouse
	python3 load_to_olap.py

# ── Analytics / Benchmarks ────────────────────────────────────────────────────

benchmark: ## Run benchmark queries across all 4 ClickHouse tables
	@echo "=== [1] Avg rating by decade (movies only) ==="
	@time $(CH_CLIENT) \
	    --query "SELECT decade, round(avg(averageRating),2) AS avg_rating, count() AS cnt \
	             FROM imdb.titles WHERE titleType='movie' AND decade IS NOT NULL \
	             GROUP BY decade ORDER BY decade"
	@echo ""
	@echo "=== [2] Top 20 genres by avg rating (min 10k votes) ==="
	@time $(CH_CLIENT) \
	    --query "SELECT primaryGenre, round(avg(averageRating),2) AS avg_rating, count() AS cnt \
	             FROM imdb.titles WHERE numVotes >= 10000 AND primaryGenre != '' \
	             GROUP BY primaryGenre ORDER BY avg_rating DESC LIMIT 20"
	@echo ""
	@echo "=== [3] Most-credited actors (pre-aggregate then join top 20) ==="
	@time $(CH_CLIENT) \
	    --query "SELECT a.nconst, p.primaryName, a.title_count \
	             FROM ( \
	               SELECT nconst, count() AS title_count \
	               FROM imdb.principals \
	               WHERE category = 'actor' \
	               GROUP BY nconst \
	               ORDER BY title_count DESC \
	               LIMIT 20 \
	             ) AS a \
	             LEFT JOIN ( \
	               SELECT nconst, primaryName \
	               FROM imdb.people \
	               WHERE nconst IN ( \
	                 SELECT nconst \
	                 FROM ( \
	                   SELECT nconst, count() AS title_count \
	                   FROM imdb.principals \
	                   WHERE category = 'actor' \
	                   GROUP BY nconst \
	                   ORDER BY title_count DESC \
	                   LIMIT 20 \
	                 ) \
	               ) \
	             ) AS p ON p.nconst = a.nconst \
	             ORDER BY a.title_count DESC"
	@echo ""
	@echo "=== [4] Alternate titles per region (top 10 regions) ==="
	@time $(CH_CLIENT) \
	    --query "SELECT region, count() AS aka_count \
	             FROM imdb.akas GROUP BY region ORDER BY aka_count DESC LIMIT 10"
	@echo ""
	@echo "=== [5] Most-credited directors (pre-aggregate then join top 20) ==="
	@time $(CH_CLIENT) \
	    --query "SELECT d.nconst, p.primaryName, d.film_count \
	             FROM ( \
	               SELECT nconst, count() AS film_count \
	               FROM imdb.principals \
	               WHERE category = 'director' \
	               GROUP BY nconst \
	               ORDER BY film_count DESC \
	               LIMIT 20 \
	             ) AS d \
	             LEFT JOIN ( \
	               SELECT nconst, primaryName \
	               FROM imdb.people \
	               WHERE nconst IN ( \
	                 SELECT nconst \
	                 FROM ( \
	                   SELECT nconst, count() AS film_count \
	                   FROM imdb.principals \
	                   WHERE category = 'director' \
	                   GROUP BY nconst \
	                   ORDER BY film_count DESC \
	                   LIMIT 20 \
	                 ) \
	               ) \
	             ) AS p ON p.nconst = d.nconst \
	             ORDER BY d.film_count DESC"

spark-benchmark: ## Run equivalent Spark queries on Parquet for comparison
	$(SPARK_SUBMIT) --conf spark.sql.shuffle.partitions=8 /opt/spark/jobs/etl_job.py --input /data/raw --output /tmp/bench_noop || true
	@echo "Run individual Spark SQL queries manually via: make spark-shell"

# ── Utilities ────────────────────────────────────────────────────────────────

ch-client: ## Open interactive ClickHouse SQL shell
	docker exec -it $(CLICKHOUSE_CONTAINER) clickhouse-client

spark-shell: ## Open Spark shell inside master container
	docker exec -it $(SPARK_MASTER_CONTAINER) /opt/spark/bin/spark-shell --master spark://spark-master:7077

clean: ## Remove generated Parquet lake (keeps raw downloads)
	rm -rf data/lake/*
	@echo "data/lake/ cleared."

clean-all: ## Remove ALL data (raw downloads + lake) — re-download required
	rm -rf data/raw/* data/lake/*
	@echo "All data cleared."

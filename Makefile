# Thin wrappers; every target is a one-liner you can also type by hand.
STAGE ?= core
# Git Bash on Windows: keep container paths (/opt/...) as-is in docker arguments
export MSYS_NO_PATHCONV = 1

.PHONY: setup stage status down smoke download pulse topology topology-kill replay e2e bench

setup:
	./setup.sh

stage:
	scripts/stage.sh $(STAGE)

status:
	docker compose ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'

down:
	scripts/stage.sh down

# P0 check: 2 live DataNodes, YARN NodeManagers registered, an MR job runs, files get 2 replicas.
smoke:
	docker compose exec -T namenode bash -c '\
	  hdfs dfsadmin -report | grep -E "^Live datanodes"; \
	  yarn node -list 2>/dev/null | grep -E "Total Nodes|RUNNING"; \
	  yarn jar $$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 4 1000 2>&1 | tail -2; \
	  head -c 100000000 /dev/urandom > /tmp/blob && hdfs dfs -put -f /tmp/blob /tmp/smoke.bin && rm /tmp/blob; \
	  hdfs fsck /tmp/smoke.bin -files -blocks -locations | grep -E "blk_|Average block replication"'

# P1: GH Archive -> HDFS raw zone. MODE=sample|demo|large
MODE ?= sample
download:
	docker compose exec -T edge scripts/download_gharchive.sh --mode $(MODE)

# Any pipeline step:  make pulse ARGS="seed-catalogue 500"
pulse:
	docker compose exec -T edge pulse $(ARGS)

# P10 streaming: submit / kill the Storm topology, replay GH Archive into Kafka
topology:
	scripts/topology.sh

topology-kill:
	docker compose exec -T storm-nimbus storm kill pulse -w 5

DT ?= 2025-09-01
H ?= 12-14
SPEED ?= 60
replay:
	docker compose exec -T edge pulse replay --date $(DT) --hours $(H) --speed $(SPEED)

# P11/P13: end-to-end run on the sample (both DAGs + streaming) with assertions
e2e:
	scripts/e2e.sh

# P13: engine benchmark on whatever data is loaded; SIZE labels the run
SIZE ?= sample
bench:
	docker compose exec -T edge pulse bench $(SIZE)

#!/usr/bin/env bash
# One-time setup: checks the machine, creates .env, downloads verified tarballs, builds images.
# Re-running is safe: downloads and builds are cached.  ./setup.sh --download-only skips the build.
set -euo pipefail
cd "$(dirname "$0")"

docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop and re-run." >&2; exit 1; }

arch=$(docker version --format '{{.Server.Arch}}')          # arm64 | amd64
mem_gb=$(( $(docker info --format '{{.MemTotal}}') / 1024 / 1024 / 1024 ))
free_gb=$(df -g . | awk 'NR==2 {print $4}')
echo "Docker: arch=$arch memory=${mem_gb}GB · free disk here=${free_gb}GB"
if [ "$mem_gb" -lt 10 ]; then
  echo "WARNING: Docker has ${mem_gb} GB. The full stages need ~11 GB (Docker Desktop > Settings > Resources)."
  echo "         On an 8 GB machine use 'make stage STAGE=lite' and the sample dataset only."
fi
[ "$free_gb" -lt 60 ] && echo "WARNING: the large dataset needs ~60 GB free disk; sample/demo need ~15 GB."

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env. Put your GitHub token in GITHUB_TOKEN= before seeding the catalogue."
fi
# Airflow runs docker compose from inside its container, so it needs this project's absolute host path
grep -q '^PROJECT_DIR=' .env || echo "PROJECT_DIR=$PWD" >> .env
set -a; . ./.env; set +a

# fetch <file> <checksum-url-base> <url>...: download once into docker/cache, trying each mirror until
# one gives a file matching the official checksum (<base>.sha512/.sha256/.sha1/.md5, strongest published).
# Checking per mirror matters: some mirrors answer 200 with an HTML error page.
fetch() {
  local name=$1 out="docker/cache/$1" sumbase=$2; shift 2
  local algo want="" len
  for algo in sha512 sha256 sha1 md5; do
    case $algo in sha512) len=128 ;; sha256) len=64 ;; sha1) len=40 ;; md5) len=32 ;; esac
    want=$(curl -fsSL "$sumbase.$algo" 2>/dev/null | tr -d ' \n' | grep -oiE "[0-9a-f]{$len}" | head -1 | tr 'A-F' 'a-f' || true)
    [ -n "$want" ] && break
  done
  [ -n "$want" ] || { echo "No checksum published for $name" >&2; exit 1; }
  sum() { openssl dgst -$algo -r "$1" | cut -d' ' -f1; }
  if [ -f "$out" ] && [ "$(sum "$out")" = "$want" ]; then echo "verified $name ($algo, cached)"; return; fi
  local url
  for url in "$@"; do
    u=${url#https://}; echo "Downloading $name from ${u%%/*}"
    if curl -fL --retry 3 -s -o "$out.part" "$url" && [ "$(sum "$out.part")" = "$want" ]; then
      mv "$out.part" "$out"; echo "verified $name ($algo)"; return
    fi
    echo "  bad or missing file from that mirror, trying next" >&2
    rm -f "$out.part"
  done
  echo "No mirror produced a valid $name" >&2; exit 1
}
# Apache release: CDN (current releases) -> Huawei mirror (fast, keeps old releases) -> archive (slow, has everything).
apache() {
  fetch "$1" "https://archive.apache.org/dist/$2" \
    "https://dlcdn.apache.org/$2" "https://repo.huaweicloud.com/apache/$2" "https://archive.apache.org/dist/$2"
}
maven() { fetch "$1" "https://repo1.maven.org/maven2/$2" "https://repo1.maven.org/maven2/$2"; }

mkdir -p docker/cache
HV=$HADOOP_VERSION
if [ "$arch" = arm64 ]; then hfile=hadoop-$HV-aarch64.tar.gz; else hfile=hadoop-$HV.tar.gz; fi
apache "hadoop-$HV-$arch.tar.gz"                 "hadoop/common/hadoop-$HV/$hfile"
apache apache-hive-3.1.3-bin.tar.gz             hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
apache pig-0.18.0.tar.gz                        pig/pig-0.18.0/pig-0.18.0.tar.gz
apache sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz     sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
apache apache-mahout-distribution-0.13.0.tar.gz mahout/0.13.0/apache-mahout-distribution-0.13.0.tar.gz
apache spark-3.5.9-bin-hadoop3.tgz              spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
apache hbase-2.5.16-hadoop3-bin.tar.gz          hbase/2.5.16/hbase-2.5.16-hadoop3-bin.tar.gz
maven  postgresql-42.7.4.jar                    org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
maven  commons-lang-2.6.jar                     commons-lang/commons-lang/2.6/commons-lang-2.6.jar
maven  json-20090211.jar                        org/json/json/20090211/json-20090211.jar
maven  javax.jdo-3.2.0-m3.jar                   org/datanucleus/javax.jdo/3.2.0-m3/javax.jdo-3.2.0-m3.jar

[ "${1:-}" = --download-only ] && exit 0

docker compose --profile build build hadoop-image
docker compose --profile build build stack-image
docker compose --profile build build airflow-image
echo "Setup done. Next: make stage STAGE=core   (or STAGE=lite on an 8 GB machine)"

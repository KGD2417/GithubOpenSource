#!/usr/bin/env bash
# Builds the Java modules in a throwaway Maven container (no JDK/Maven needed on the laptop).
#   scripts/mvn.sh -q package        -> java/*/target/*.jar
set -euo pipefail
cd "$(dirname "$0")/.."
docker run --rm -v "$PWD":/src -v pulse-m2:/root/.m2 -w /src/java maven:3.9.9-eclipse-temurin-17 mvn -B "$@"

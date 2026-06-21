#!/usr/bin/env bash
# run-tests.sh — Orchestration du test e2e pour datacat-reader.
#
# Ce script :
#   1. Démarre un conteneur MinIO (datacat-reader-minio-test) sur les ports 9200/9201.
#   2. Attend que MinIO soit disponible.
#   3. Lance `cargo test -- --nocapture`.
#   4. Arrête et supprime le conteneur MinIO (quoi qu'il arrive).
#
# Prérequis :
#   - Docker daemon en cours d'exécution.
#
# Usage :
#   cd reader && ./run-tests.sh

set -euo pipefail

CONTAINER="datacat-reader-minio-test"
MINIO_API_PORT=9200
MINIO_CONSOLE_PORT=9201
MINIO_IMAGE="minio/minio:latest"

# ── Couleurs ──────────────────────────────────────────────────────────────────
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
NC="\033[0m"

info()  { echo -e "${GREEN}[run-tests]${NC} $*"; }
warn()  { echo -e "${YELLOW}[run-tests]${NC} $*"; }
error() { echo -e "${RED}[run-tests]${NC} $*" >&2; }

# ── Nettoyage à la sortie ─────────────────────────────────────────────────────
cleanup() {
    info "Arrêt du conteneur MinIO…"
    docker rm -f "$CONTAINER" 2>/dev/null || true
    info "Nettoyage terminé."
}
trap cleanup EXIT

# ── Démarrage de MinIO ────────────────────────────────────────────────────────
info "Démarrage du conteneur MinIO '$CONTAINER'…"
docker rm -f "$CONTAINER" 2>/dev/null || true

docker run -d \
    --name "$CONTAINER" \
    -p "${MINIO_API_PORT}:9000" \
    -p "${MINIO_CONSOLE_PORT}:9001" \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "$MINIO_IMAGE" \
    server /data --console-address ":9001"

info "Attente de la disponibilité de MinIO…"
MAX_WAIT=30
WAITED=0
until curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; do
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        error "MinIO non disponible après ${MAX_WAIT}s"
        docker logs "$CONTAINER"
        exit 1
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done
info "MinIO disponible (attendu ${WAITED}s)."

# ── Lancement des tests ───────────────────────────────────────────────────────
info "Lancement des tests cargo…"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

cargo test -- --nocapture

TEST_EXIT=$?

if [ "$TEST_EXIT" -eq 0 ]; then
    info "Tous les tests PASSENT."
else
    error "Tests ÉCHOUÉS (code $TEST_EXIT)."
    exit "$TEST_EXIT"
fi

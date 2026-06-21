# datacat-exporter

Export périodique Datacat (PostgreSQL → Parquet sur S3-compatible).

## Usage

```bash
datacat-export --table events --date 2024-06-15 --bucket my-bucket
datacat-export --table logs   --date 2024-06-15 --bucket my-bucket --prefix staging
```

## Configuration (variables d'environnement)

| Variable                | Obligatoire | Description |
|---|---|---|
| `DATABASE_URL`          | oui         | URL PostgreSQL (`postgres://user:pass@host:port/db`) |
| `AWS_ACCESS_KEY_ID`     | oui         | Clé d'accès S3 |
| `AWS_SECRET_ACCESS_KEY` | oui         | Secret S3 |
| `S3_BUCKET`             | non*        | Bucket par défaut (sinon `--bucket`) |
| `S3_ENDPOINT`           | non         | URL du endpoint S3 (vide = AWS S3 ; ex. `http://localhost:9100` pour MinIO) |
| `S3_REGION`             | non         | Région S3 (défaut : `eu-west-1`) |
| `S3_ALLOW_HTTP`         | non         | `true` pour autoriser HTTP (MinIO dev sans TLS) |
| `S3_PREFIX`             | non         | Préfixe de clé S3 (ex. `prod`) |

## Tests e2e locaux

```bash
cd exporter
./run-tests.sh
```

Le script démarre MinIO via Docker (port 9100), exécute les tests Rust, puis supprime le
conteneur. PostgreSQL doit tourner sur `localhost:55432` (identifiants `datacat`/`datacat`).

## Layout S3

```
s3://<bucket>/[prefix/]events/date=YYYY-MM-DD/part-0000.parquet
s3://<bucket>/[prefix/]logs/date=YYYY-MM-DD/part-0000.parquet
```

Voir `docs/cold-storage.md` pour le schéma Parquet complet, la stratégie d'idempotence et
les exemples de lecture DataFusion / DuckDB.

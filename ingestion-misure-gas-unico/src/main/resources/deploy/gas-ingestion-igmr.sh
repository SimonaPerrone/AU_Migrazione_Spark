#!/usr/bin/env bash

DEPLOY_PATH=${deploy.path}

echo "Running IGMR flow..."
"$DEPLOY_PATH"/spark-submit-gas-ingestion.sh -f IGMR && "$DEPLOY_PATH"/sqoop/igmr_export.sh

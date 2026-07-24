#!/usr/bin/env bash

DEPLOY_PATH=${deploy.path}

echo "Running IGMG flow..."
"$DEPLOY_PATH"/spark-submit-gas-ingestion.sh -f IGMG && "$DEPLOY_PATH"/sqoop/igmg_export.sh

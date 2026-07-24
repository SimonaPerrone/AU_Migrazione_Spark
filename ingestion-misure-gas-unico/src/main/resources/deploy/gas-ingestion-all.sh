#!/usr/bin/env bash

DEPLOY_PATH=${deploy.path}

"$DEPLOY_PATH"/gas-ingestion-igmg.sh
"$DEPLOY_PATH"/gas-ingestion-igmr.sh
"$DEPLOY_PATH"/gas-ingestion-standard.sh


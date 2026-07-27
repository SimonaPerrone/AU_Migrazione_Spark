#!/bin/bash

CURDD=$(date +%Y-%m-%d)
YESTERDAY=$(date +%Y-%m-%d -d "$CURDD -1 day")

${deploy.publish_report_script.path}/publish_report.sh -f 1G -d YESTERDAY
${deploy.publish_report_script.path}/publish_report.sh -f 2G -d YESTERDAY
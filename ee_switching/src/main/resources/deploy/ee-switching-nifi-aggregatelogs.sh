#!/bin/bash

#param1: logPath, param2: logFilename

for file in $( ls $1/nifi_??????_*.log | sort ); do
  cat "$file" >> $1/$2
  rm   "$file"
done

#!/bin/bash

TGL=""
RGL=""
DEF=""
FUI=""
IM1=""
RML=""
RSL=""
SW1=""
TAL=""
TAS=""
TAV=""
TML=""
TMV=""

echo "flusso:$1"

if [ $1 == "TGL" ]; then
  TGL="current_timestamp as " 
fi

if [ $1 == "RGL" ]; then
  RGL="current_timestamp as " 
fi

if [ $1 == "DEF" ]; then
  DEF="current_timestamp as " 
fi

if [ $1 == "FUI" ]; then
  FUI="current_timestamp as "
fi
if [ $1 == "IM1" ]; then
  IM1="current_timestamp as "
fi
if [ $1 == "RML" ]; then
  RML="current_timestamp as "
fi
if [ $1 == "RSL" ]; then
  RSL="current_timestamp as "
fi
if [ $1 == "SW1" ]; then
  SW1="current_timestamp as "
fi
if [ $1 == "TAL" ]; then
  TAL="current_timestamp as "
fi
if [ $1 == "TAS" ]; then
  TAS="current_timestamp as "
fi
if [ $1 == "TAV" ]; then
  TAV="current_timestamp as "
fi
if [ $1 == "TML" ]; then
  TML="current_timestamp as "
fi
if [ $1 == "TMV" ]; then
  TMV="current_timestamp as "
fi

hive -e "$(cat compacting_update.sql.in | \
sed "s/__TGL__/${TGL}/g" | \
sed "s/__RGL__/${RGL}/g" | \
sed "s/__DEF__/${DEF}/g" | \
sed "s/__FUI__/${FUI}/g" | \
sed "s/__IM1__/${IM1}/g" | \
sed "s/__RML__/${RML}/g" | \
sed "s/__RSL__/${RSL}/g" | \
sed "s/__SW1__/${SW1}/g" | \
sed "s/__TAL__/${TAL}/g" | \
sed "s/__TAS__/${TAS}/g" | \
sed "s/__TAV__/${TAV}/g" | \
sed "s/__TML__/${TML}/g" | \
sed "s/__TMV__/${TMV}/g")"

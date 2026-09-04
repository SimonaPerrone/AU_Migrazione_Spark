#!/usr/bin/env bash
##set -euo pipefail

ROOT="Documenti per migrazione"
TEMPLATE="$ROOT/Template/0.TEMPLATE_MIGRAZIONE_STEP1.md"
OUT_DIR="$ROOT/Migrazione Step1"

mkdir -p "$OUT_DIR"

declare -A SIGLA=(
  ["aggiustamento-gas"]="AGG"
  ["cce-calcolo"]="CCE"
  ["pubblicazione_cce"]="CCE_PUB"
  ["ccg-pubblicazione"]="CCG"
  ["cdp-codprofstd-tds"]="CDP"
  ["aggregatore_cdp"]="CDP_AGG"
  ["cagas"]="CDP_CAGAS"
  ["freezer_pre_calcolo"]="CDP_PRE"
  ["indennizzi-misure-gas"]="CIG"
  ["calcolo-capacita"]="CLG"
  ["pubblicazione_pcg"]="CLG_PUB"
  ["ee_aggregato_cloudera"]="EE_AGG"
  ["erp-e-profilazione-punti-no"]="ERP"
  ["gse-energy-release"]="GSE"
  ["gsv-gasivori"]="GSV"
  ["scambio-dati-gasivori"]="GSV_SCAMBIO"
  ["ingestion-elettrico-ammissibilita"]="ING_EE"
  ["ingestion-misure-gas-unico"]="ING_GAS"
  ["meccanismo-incentivante-distributori-gas"]="MID"
  ["portale-consumi"]="PDC"
  ["portale-consumi-2.0"]="PDC_2"
  ["portale-consumi-common"]="PDC_COMM"
  ["portale-consumi-ee"]="PDC_EE"
  ["sbg-sessione-bilanciamento-gas"]="SBG"
  ["aggiustamento-bilanciamento-gas"]="SBG_AGG"
  ["aggregatore-consumi-agg"]="SBG_CONS"
  ["sgs-flusso-storico-gas"]="SGS"
  ["ee_switching"]="SW"
  ["trasmissione-settlement-gas"]="TSG"
  ["gas_trasmissionemisure_cloudera"]="TSG_CLOU"
  ["delete-old-partition"]="DOP"
  ["partition-optimization"]="PART_OPT"
)

projects=(
  aggiustamento-bilanciamento-gas
  aggiustamento-gas
  aggregatore-consumi-agg
  aggregatore_cdp
  cagas
  calcolo-capacita
  cce-calcolo
  ccg-pubblicazione
  cdp-codprofstd-tds
  delete-old-partition
  ee_aggregato_cloudera
  ee_switching
  erp-e-profilazione-punti-no
  freezer_pre_calcolo
  gas_trasmissionemisure_cloudera
  gse-energy-release
  gsv-gasivori
  indennizzi-misure-gas
  ingestion-elettrico-ammissibilita
  ingestion-misure-gas-unico
  meccanismo-incentivante-distributori-gas
  partition-optimization
  portale-consumi
  portale-consumi-2.0
  portale-consumi-common
  portale-consumi-ee
  pubblicazione_cce
  pubblicazione_pcg
  sbg-sessione-bilanciamento-gas
  scambio-dati-gasivori
  sgs-flusso-storico-gas
  trasmissione-settlement-gas
)

for p in "${projects[@]}"; do
  sigla="${SIGLA[$p]:-$(echo "$p" | tr '[:lower:]-.' '[:upper:]__')}"
  out="$OUT_DIR/[$sigla] - $p.md"

  content="$(cat "$TEMPLATE")"
  content="${content//\[NOME_PROGETTO\]/$p}"
  content="${content//\[PROGETTO\]/$p}"

  printf "%s\n" "$content" > "$out"
done

echo "Creati ${#projects[@]} documenti in: $OUT_DIR"
#!/usr/bin/env bash
set -euo pipefail

# === PATH REPO (Git Bash) ===
REPO_ROOT="/c/LAVORO/GitHub/AU_Migrazione_Spark"
DOC_ROOT="$REPO_ROOT/Documenti per migrazione"
TEMPLATE="$DOC_ROOT/Template/0.TEMPLATE_MIGRAZIONE_STEP1.md"
SIGLA_FILE="$DOC_ROOT/Sigla Progetti.md"
OUT_DIR="$DOC_ROOT/Migrazione Step1"

mkdir -p "$OUT_DIR"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Template non trovato: $TEMPLATE" >&2
  exit 1
fi

# Carica mappa sigle da file CSV (se presente)
declare -A SIGLA
if [[ -f "$SIGLA_FILE" ]]; then
  while IFS=';' read -r progetto sigla; do
    [[ -z "${progetto// }" ]] && continue
    [[ "$progetto" == "Progetto git" ]] && continue
    progetto="$(echo "$progetto" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    sigla="$(echo "$sigla" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -n "$progetto" && -n "$sigla" ]] && SIGLA["$progetto"]="$sigla"
  done < "$SIGLA_FILE"
fi

count=0
for dir in "$REPO_ROOT"/*; do
  [[ -d "$dir" ]] || continue
  project="$(basename "$dir")"

  # Escludi cartelle non-progetto
  [[ "$project" == ".git" ]] && continue
  [[ "$project" == "Documenti per migrazione" ]] && continue

  sigla="${SIGLA[$project]:-$(echo "$project" | tr '[:lower:]-.' '[:upper:]__')}"
  out="$OUT_DIR/[$sigla] - $project.md"

  content="$(cat "$TEMPLATE")"
  content="${content//\[NOME_PROGETTO\]/$project}"
  content="${content//\[PROGETTO\]/$project}"

  printf "%s\n" "$content" > "$out"
  count=$((count + 1))
done

echo "Creati $count documenti in: $OUT_DIR"

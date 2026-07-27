#!/usr/bin/env bash
set -e

# Oozie hack
[[ "${PWD}" == *"/yarn/nm"* ]] && DEPLOY_PATH="${PWD}" || DEPLOY_PATH="$(dirname "$(realpath "${0}")")"

die()
{
    echo "$*" >&2
    exit 2
}

needs_arg()
{
    if [ -z "$OPTARG" ];
    then
        die "No arg for --$OPT option"
    fi
}

print_help()
{
    echo "ee-switching-file.sh"
    echo "                                [-h|--help]"
    echo "                                [-t|--timestamp=filepath"
    echo "                                [-p|--pod=filepath"
    echo "                                [-s|--date-storici-switching=filepath"
    echo "                                [-f|--date-funzionali-switching=filepath"
    echo "                                [-F|--date-funzionali-na=filepath"
    echo "                                [-d|--piva-distributore=filepath"
    echo "                                [-D|--piva-udd=filepath"
    echo "                                [-c|--coppie-piva=filepath"
    echo "                                [-q|--queue=queue"
}


declare -a ARRAY_PARAMETRI_FUNZIONALI
declare -a ARRAY_PARAMETRI_STORICI

while getopts ht:p:s:f:F:d:D:c:q:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    h | help ) PRINT_HELP="true" ;;
    t | timestamp ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-t" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-t" "$OPTARG") ;;
    p | pod ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-p" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-p" "$OPTARG") ;;
    s | date-storici-switching ) needs_arg; ARRAY_PARAMETRI_STORICI+=("-s" "$OPTARG") ;;
    f | date-funzionali-switching ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-f" "$OPTARG") ;;
    F | date-funzionali-na ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-F" "$OPTARG") ;;
    d | piva-distributore ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-d" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-d" "$OPTARG") ;;
    D | piva-udd ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-D" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-D" "$OPTARG") ;;
    c | coppie-piva ) needs_arg; ARRAY_PARAMETRI_FUNZIONALI+=("-c" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-c" "$OPTARG") ;;
    q | queue ) ARRAY_PARAMETRI_FUNZIONALI+=("-q" "$OPTARG"); ARRAY_PARAMETRI_STORICI+=("-q" "$OPTARG") ;;
    ??* ) die "Illegal option --$OPT" ;;
    \? ) exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ ${PRINT_HELP} ]]; then
    print_help
    exit 0
fi

"${DEPLOY_PATH}/ee-switching-file-funzionali.sh" "${ARRAY_PARAMETRI_FUNZIONALI[@]}" # &
"${DEPLOY_PATH}/ee-switching-file-storici.sh" "${ARRAY_PARAMETRI_STORICI[@]}" # &
# wait
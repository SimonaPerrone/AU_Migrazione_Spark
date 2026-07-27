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
    echo "ee-switching-file-funzionali.sh"
    echo "                                [-h|--help]"
    echo "                                [-t|--timestamp=filepath"
    echo "                                [-p|--pod=filepath"
    echo "                                [-f|--date-funzionali-switching=filepath"
    echo "                                [-F|--date-funzionali-na=filepath"
    echo "                                [-d|--piva-distributore=filepath"
    echo "                                [-D|--piva-udd=filepath"
    echo "                                [-c|--coppie-piva=filepath"
    echo "                                [-q|--queue=queue"
}

while getopts ht:p:f:F:d:D:c:q:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    h | help ) PRINT_HELP="true" ;;
    t | timestamp ) needs_arg ;;
    p | pod ) needs_arg ;;
    f | date-funzionali-switching ) needs_arg ;;
    F | date-funzionali-na ) needs_arg ;;
    d | piva-distributore ) needs_arg; ;;
    D | piva-udd ) needs_arg ;;
    c | coppie-piva ) needs_arg ;;
    q | queue ) needs_arg ;;
    ??* ) die "Illegal option --$OPT" ;;
    \? ) exit 2 ;;
  esac
done

if [[ ${PRINT_HELP} ]]; then
    print_help
    exit 0
fi

${DEPLOY_PATH}/spark-submit-ee-switching-generazione-file.sh --flow-name=funzionali "$@"
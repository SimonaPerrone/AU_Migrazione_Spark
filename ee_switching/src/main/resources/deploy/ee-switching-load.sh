#!/usr/bin/env bash
set -e
set -o pipefail

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
    echo "ee-switching-load.sh"
    echo "                      [-h|--help]"
    echo "                      [-v|--verbose]"
    echo "                      [--debug]"
    echo "                      [-p|--pod=XXX[,YYY,...]]"
    echo "                      [-s|--date-storici-switching=2020015|20200101,20200201"
    echo "                      [-f|--date-funzionali-switching=20200101[,20200201,...]"
    echo "                      [-F|--date-funzionali-na=20200101[,20200201,...]"
    echo "                      [-d|--piva-distributore=XXX[,YYY,...]"
    echo "                      [-D|--piva-udd=XXX[,YYY,...]"
    echo "                      [-c|--coppie-piva=XXX,YYY[-AAA,BBB-...]"
}

while getopts hvp:s:f:F:d:D:c:-: OPT; do
  if [ "$OPT" = "-" ]; then
    OPT="${OPTARG%%=*}"
    OPTARG="${OPTARG#$OPT}"
    OPTARG="${OPTARG#=}"
  fi
  case "$OPT" in
    h | help ) PRINT_HELP="true" ;;
    v | verbose ) PRINT_VERBOSE="true" ;;
    debug ) PRINT_DEBUG="true" ;;
    p | pod ) needs_arg; PODS="$OPTARG" ;;
    s | date-storici-switching ) needs_arg; STORICI_SWITCHING_DATES="$OPTARG" ;;
    f | date-funzionali-switching ) needs_arg; FUNZIONALI_SWITCHING_DATES="$OPTARG" ;;
    F | date-funzionali-na ) needs_arg; FUNZIONALI_NEW_ACTIVATION_DATES="$OPTARG" ;;
    d | piva-distributore ) needs_arg; PIVAS_DISTR="$OPTARG" ;;
    D | piva-udd ) needs_arg; PIVAS_UDD="$OPTARG" ;;
    c | coppie-piva ) needs_arg; PIVAS_COUPLES="$OPTARG" ;;
    ??* ) die "Illegal option --$OPT" ;;
    \? ) exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [[ ${PRINT_HELP} ]]; then
    print_help
    exit 0
fi

# load arrays
declare -a STORICI_PARAMETERS_ARRAY
declare -a FUNZIONALI_SWITCHING_PARAMETERS_ARRAY
declare -a FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY

if [[ -v PRINT_VERBOSE ]]; then
    STORICI_PARAMETERS_ARRAY+=("-v")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-v")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-v")
fi

if [[ -v PRINT_DEBUG ]]; then
    STORICI_PARAMETERS_ARRAY+=("--debug")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("--debug")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("--debug")
fi

if [[ -v PODS ]]; then
    STORICI_PARAMETERS_ARRAY+=("-p" "${PODS}")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-p" "${PODS}")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-p" "${PODS}")
fi

if [[ -v STORICI_SWITCHING_DATES ]]; then
    STORICI_PARAMETERS_ARRAY+=("-s" "${STORICI_SWITCHING_DATES}")
fi

if [[ -v FUNZIONALI_SWITCHING_DATES ]]; then
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-f" "${FUNZIONALI_SWITCHING_DATES}" )
fi

if [[ -v FUNZIONALI_NEW_ACTIVATION_DATES ]]; then
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-F" "${FUNZIONALI_NEW_ACTIVATION_DATES}")
fi

if [[ -v PIVAS_DISTR ]]; then
    STORICI_PARAMETERS_ARRAY+=("-d" "${PIVAS_DISTR}")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-d" "${PIVAS_DISTR}")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-d" "${PIVAS_DISTR}")
fi

if [[ -v PIVAS_UDD ]]; then
    STORICI_PARAMETERS_ARRAY+=("-D" "${PIVAS_UDD}")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-D" "${PIVAS_UDD}")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-D" "${PIVAS_UDD}")
fi

if [[ -v PIVAS_COUPLES ]]; then
    STORICI_PARAMETERS_ARRAY+=("-c" "${PIVAS_COUPLES}")
    FUNZIONALI_SWITCHING_PARAMETERS_ARRAY+=("-c" "${PIVAS_COUPLES}")
    FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY+=("-c" "${PIVAS_COUPLES}")
fi

"${DEPLOY_PATH}/ee-switching-load-storici.sh" \
    "${STORICI_PARAMETERS_ARRAY[@]}" \
    &
"${DEPLOY_PATH}/ee-switching-load-funzionali-switching.sh" \
    "${FUNZIONALI_SWITCHING_PARAMETERS_ARRAY[@]}" \
    &
"${DEPLOY_PATH}/ee-switching-load-funzionali-na.sh" \
    "${FUNZIONALI_NEW_ACTIVATION_PARAMETERS_ARRAY[@]}" \
    &
wait

#!/bin/bash

echo "SGS workflow started."

# Funzione per eseguire uno script e gestire errori
run_step() {
    local step_name="$1"
    local script_path="$2"

    echo "Starting $step_name..."
    $script_path
    if [ $? -ne 0 ]; then
        echo "$step_name exited with error."
        exit 2
    fi
}

# Esegui i vari step
run_step "Perimetro calculation" "${deploy.path}/run_sgs_perimetro.sh"
run_step "Aggregation" "${deploy.path}/run_sgs_aggregazione.sh"
run_step "Publish" "${deploy.path}/run_sgs_pubblicazione.sh"

echo "SGS workflow completed successfully."

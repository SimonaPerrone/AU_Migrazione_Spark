#!/usr/bin/env bash
set -e

# load bash JDBC variables from Java properties file in HDFS
source <(${sqoop.cat.command} '${sqoop.properties.hdfs.path}' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table '${sqoop.export.table}' \
    --columns 't_codice_pdr,t_cau_int_mis,t_cau_int_cor,t_matricola_misuratore,t_tipo_misuratore,t_telegestito,n_coeff_correzione,t_classe_misuratore,n_access_misuratore,n_num_cifre_misuratore,t_anno_fabbric_misuratore,d_data_inst_misuratore,t_misuratore_integrato,t_presenza_convertitore,t_matricola_convertitore,n_num_cifre_convertitore,t_anno_fabbric_convertitore,d_data_inst_convertitore,n_pressione_misura,d_data_misura,t_pivautente,t_distributore,t_nomefile,t_ammissibilita,d_data_inservizio_sm,t_path,mot_ret_let' \
    --export-dir '${sqoop.export.igmr.dir}' \
    --input-fields-terminated-by ';' \
    --input-lines-terminated-by '\n' \
    --input-null-string '${sqoop.null.character}' \
    --input-null-non-string '${sqoop.null.character}' \
    --staging-table '${sqoop.staging.table}' \
    --clear-staging-table \
    --num-mappers ${sqoop.num.mappers}

# clear Hive staging table if the sqoop export finished successfully
hdfs dfs \
    -rm \
    -f \
    -skipTrash \
    "${sqoop.export.igmr.dir}/*"

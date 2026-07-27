query=$(cat <<-END
SELECT \\\`(d_creazione_next)?+.+\\\` \
FROM ( \
        SELECT *,lead(d_creazione) \
        over (partition BY pod14,d_data_decorrenza ORDER BY d_creazione ASC) AS d_creazione_next \
         FROM \\\${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.\\\${hiveconf:SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME} \
         where 1=1 \
         $([[ "${CHECK_ANNO_MESE}" != "na" ]] && echo -n "and ${CHECK_ANNO_MESE}") \
         $([[ "${CHECK_POD}" != "na" ]] && echo -n "and ${CHECK_POD}") \
         $([[ "${CHECK_PIVA_DISTR}" != "na" ]] && echo -n "and ${CHECK_PIVA_DISTR}") \
         $([[ "${CHECK_PIVA_UDD}" != "na" ]] && echo -n "and ${CHECK_PIVA_UDD}") \
         $([[ "${CHECK_SINGLE_DATA_DECORRENZA}" != "na" ]] && echo -n "and ${CHECK_SINGLE_DATA_DECORRENZA}") \
         $([[ "${CHECK_PAIR_PIVA_DIST_PIVA_UDD}" != "na" ]] && echo -n "and ${CHECK_PAIR_PIVA_DIST_PIVA_UDD}") \
         )t1 \
WHERE t1.d_creazione_next IS NULL
END
)
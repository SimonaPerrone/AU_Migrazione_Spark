export CREATE_VIEW_PDR=$(cat <<-EOF

CREATE OR REPLACE FORCE VIEW ${oracle.table.clg_perimetro_pdr_gm_view.db}.${oracle.table.clg_perimetro_pdr_gm_view.name}
(
   DATA_CALC,
   N_ID_PDR,
   T_CODICE_PDR,
   N_PRELIEVO_ANNUO,
   N_ID_REMI,
   T_TRATTAMENTO_SETTLEMENT,
   T_COD_CAT_USO
)
AS
   SELECT DATA_CALC,
          N_ID_PDR,
          T_CODICE_PDR,
          N_PRELIEVO_ANNUO,
          to_char(N_ID_REMI),
          T_TRATTAMENTO_SETTLEMENT,
          T_COD_CAT_USO
     FROM TMP_CLG_PERIMETRO_PDR
EOF
)

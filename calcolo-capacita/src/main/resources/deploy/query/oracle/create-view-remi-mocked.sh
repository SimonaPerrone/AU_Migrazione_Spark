export CREATE_VIEW_REMI=$(cat <<-EOF

CREATE OR REPLACE FORCE VIEW ${oracle.table.clg_perimetro_remi_gm_view.db}.${oracle.table.clg_perimetro_remi_gm_view.name}
(
   DATA_CALC,
   ANNO,
   N_ID_REMI,
   T_REMI,
   N_ID_REMI_ANAGRAFICA,
   T_TIPO_REMI,
   T_Z,
   T_PMAX
)
AS
   SELECT DATA_CALC,
          ANNO,
          to_char(N_ID_REMI),
          T_REMI,
          N_ID_REMI_ANAGRAFICA,
          T_TIPO_REMI,
          N_Z,
          N_PMAX
     FROM TMP_CLG_PERIMETRO_remi
EOF
)

package it.eng.au.ammissibilitaSettlementGas.model

case class TFCFile(
                  n_id_tsg2_file: Long,
                  nome_file: String,
                  piva_rdb: Option[String],
                  annomese: Option[String],
                  data_creazione: String,
                  progressivo: String,
                  execution_id: BigInt
                  ) extends Serializable

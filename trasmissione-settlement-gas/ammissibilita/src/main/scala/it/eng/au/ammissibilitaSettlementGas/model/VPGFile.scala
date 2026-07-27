package it.eng.au.ammissibilitaSettlementGas.model

case class VPGFile(
                    n_id_tsg2_file: Long,
                    nome_file: String,
                    piva_rdb: Option[String],
                    annotermico: Option[String],
                    data_creazione: String,
                    progressivo: String,
                    execution_id: BigInt
                  ) extends Serializable

package it.eng.au.ammissibilitaSettlementGas.model

case class TFCAmm(
                 num_riga: String,
                 cod_tipo_file: String,
                 piva_utente: String,
                 verifica_amm: Boolean,
                 cod_causale: String,
                 motivazione: String
                 ) extends Serializable

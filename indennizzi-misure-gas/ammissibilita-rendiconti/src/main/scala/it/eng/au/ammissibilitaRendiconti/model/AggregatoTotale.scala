package it.eng.au.ammissibilitaRendiconti.model

case class AggregatoTotale(
                            id_indennizzo: Long,
                            piva_id: String,
                            piva_udd: String,
                            om1_sii: Option[Double],
                            om2_sii: Option[Double],
                            om3_sii: Option[Double]
                          ) extends Serializable

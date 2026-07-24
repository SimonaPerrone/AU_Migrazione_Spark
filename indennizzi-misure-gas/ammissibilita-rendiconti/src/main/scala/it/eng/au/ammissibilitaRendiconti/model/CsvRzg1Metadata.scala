package it.eng.au.ammissibilitaRendiconti.model

case class CsvRzg1Metadata(
                            fileName: String,
                            header: String,
                            data: Option[String],
                            id_indennizzo: Option[String],
                            piva_id: Option[String],
                            rag_soc_id: Option[String],
                            piva_udd: Option[String],
                            rag_soc_udd: Option[String],
                            om1_id: Option[String],
                            om2_id: Option[String],
                            om3_id: Option[String]
                          ) extends Serializable

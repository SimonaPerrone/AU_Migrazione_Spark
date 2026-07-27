package it.sferanet.au.model

case class FlowFields(service: String, //flusso
                      pdr: String, //cod_prd
                      date: String, //date
                      pivaDistr: String, //piva distr
                      pivaUtente: String, //piva utente
                      measure: String, //Dato Misura 1
                      converted: String, //Dato Misura 2
                      serialNumberMis: String, //matricola misuratore
                      serialNumberConv: String, //matricola convertitore
                      local_file: String,
                      d_caricamento: String,
                      ammissibilita: String
                     ) extends Serializable {
  def getValues: List[String] = {
    List[String](
      service,
      pdr,
      date,
      pivaDistr,
      pivaUtente,
      measure,
      converted,
      serialNumberMis,
      serialNumberConv,
      local_file,
      d_caricamento,
      ammissibilita
    )
  }
}
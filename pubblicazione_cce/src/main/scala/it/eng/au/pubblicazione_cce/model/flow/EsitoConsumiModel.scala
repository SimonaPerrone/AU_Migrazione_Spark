package it.eng.au.pubblicazione_cce.model.flow

// elementi necessari per la tabella Esiti provenienti dal calcolo delle misure
case class EsitoConsumiModel(
                          richiesta: String,
                          nZipFiles: Int,
                          zipFiles: List[String],
                          execution_id_input_read: String
                        )

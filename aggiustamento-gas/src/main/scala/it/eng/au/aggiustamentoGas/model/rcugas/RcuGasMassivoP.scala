package it.eng.au.aggiustamentoGas.model.rcugas

import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import org.joda.time.DateTime

case class RcuGasMassivoP(
                          startDate: DateTime,
                          endDate: DateTime,
                          tCodicePdr: String,
                          nIdPdr: String,
                          @deprecated("use RCUGAS_PDR_DATIPRELIEVO.RCUGAS_VAR_TRATTAMENTO", "20/05/2021") tTrattamento: Treatment.Value,
                          pivaUdd: Option[String],
                          tTipoFornitura: Option[String],
                          tComuneIstatPdr: Option[String],
                          tComuneIstatForn: Option[String]
                        )

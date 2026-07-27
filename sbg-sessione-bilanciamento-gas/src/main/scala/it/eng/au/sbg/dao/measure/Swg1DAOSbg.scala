package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, dateLoadFormatter, parseDateToOption}
import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, Swg1DAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Swg1}
import it.eng.au.aggiustamentoGas.schema.measure.Swg1Schema
import org.apache.spark.sql.Row

import scala.util.Try

class Swg1DAOSbg extends Swg1DAO {
  override val columns: List[String] = List(
    Swg1Schema.cod_servizio,
    Swg1Schema.cod_flusso,
    Swg1Schema.cod_pdr,
    Swg1Schema.data_prest,
    Swg1Schema.let_tot_prel,
    Swg1Schema.let_tot_conv,
    Swg1Schema.matr_mis,
    Swg1Schema.matr_conv,
    Swg1Schema.local_file,
    Swg1Schema.tipo_lettura,
    Swg1Schema.d_caricamento,
    Swg1Schema.piva_distr,
    Swg1Schema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Swg1(
      service = r.getAs[String](Swg1Schema.cod_servizio).toUpperCase,
      flowCode = Option(r.getAs[String](Swg1Schema.cod_flusso)),
      pdr = r.getAs(Swg1Schema.cod_pdr).toString,
      readType = Try(Option(r.getAs[String](Swg1Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(r.getAs[String](Swg1Schema.data_prest)),
      measure = MeasureDAO.getDoubleField(r.getAs[String](Swg1Schema.let_tot_prel)),
      converted = MeasureDAO.getDoubleField(r.getAs[String](Swg1Schema.let_tot_conv)),
      serialNumberMis = Option(r.getAs[String](Swg1Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Swg1Schema.matr_conv)),
      localFile = Option(r.getAs[String](Swg1Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Swg1Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Swg1Schema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](Swg1Schema.d_caricamento), dateLoadFormatter)
    )
  }
}

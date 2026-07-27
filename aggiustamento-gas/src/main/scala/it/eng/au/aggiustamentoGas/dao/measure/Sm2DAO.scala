package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Sm2}
import it.eng.au.aggiustamentoGas.schema.measure.Sm2Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Sm2DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getSm2ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    Sm2Schema.cod_servizio,
    Sm2Schema.cod_pdr,
    Sm2Schema.data_ril,
    Sm2Schema.data_prest,
    Sm2Schema.tipo_lettura,
    Sm2Schema.segn_mis,
    Sm2Schema.segn_conv,
    Sm2Schema.let_tot_prel,
    Sm2Schema.let_tot_conv,
    Sm2Schema.matr_mis,
    Sm2Schema.matr_conv,
    Sm2Schema.local_file,
    Sm2Schema.d_caricamento,
    Sm2Schema.piva_distr,
    Sm2Schema.piva_utente,
    Sm2Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1') && f.readType == Some('E')) || (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](Sm2Schema.data_ril)
    val dateStdFlow = r.getAs[String](Sm2Schema.data_prest)
    val segnMisSost = r.getAs[String](Sm2Schema.segn_mis)
    val segnConv = r.getAs[String](Sm2Schema.segn_conv)
    val letTotPrel = r.getAs[String](Sm2Schema.let_tot_prel)
    val letTotConv = r.getAs[String](Sm2Schema.let_tot_conv)
    var esito = r.getAs[String](Sm2Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    Sm2(
      service = r.getAs[String](Sm2Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](Sm2Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](Sm2Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](Sm2Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Sm2Schema.matr_conv)),
      localFile = Option(r.getAs[String](Sm2Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Sm2Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Sm2Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Sm2Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None))
  }
}

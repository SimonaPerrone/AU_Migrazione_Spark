package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, M01}
import it.eng.au.aggiustamentoGas.schema.measure.M01Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class M01DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getM01ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    M01Schema.cod_servizio,
    M01Schema.cod_pdr,
    M01Schema.data_lettura,
    M01Schema.data_prest,
    M01Schema.tipo_lettura,
    M01Schema.segn_mis,
    M01Schema.segn_conv,
    M01Schema.let_tot_prel,
    M01Schema.let_tot_conv,
    M01Schema.matr_mis,
    M01Schema.matr_conv,
    M01Schema.local_file,
    M01Schema.d_caricamento,
    M01Schema.piva_distr,
    M01Schema.piva_utente,
    M01Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](M01Schema.data_lettura)
    val dateStdFlow = r.getAs[String](M01Schema.data_prest)
    val segnMisSost = r.getAs[String](M01Schema.segn_mis)
    val segnConv = r.getAs[String](M01Schema.segn_conv)
    val letTotPrel = r.getAs[String](M01Schema.let_tot_prel)
    val letTotConv = r.getAs[String](M01Schema.let_tot_conv)
    var esito = r.getAs[String](M01Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    M01(
      service = r.getAs[String](M01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](M01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](M01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](M01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](M01Schema.matr_conv)),
      localFile = Option(r.getAs[String](M01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](M01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](M01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](M01Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}

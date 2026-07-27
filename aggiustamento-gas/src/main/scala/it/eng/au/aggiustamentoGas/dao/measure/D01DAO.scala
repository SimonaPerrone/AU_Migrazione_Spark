package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{D01, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.D01Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class D01DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getD01ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    D01Schema.cod_servizio,
    D01Schema.cod_pdr,
    D01Schema.data_disattivazione,
    D01Schema.data_prest,
    D01Schema.tipo_lettura,
    D01Schema.segn_mis,
    D01Schema.segn_conv,
    D01Schema.let_tot_prel,
    D01Schema.let_tot_conv,
    D01Schema.matr_mis,
    D01Schema.matr_conv,
    D01Schema.local_file,
    D01Schema.d_caricamento,
    D01Schema.piva_distr,
    D01Schema.piva_utente,
    D01Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1')) || (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](D01Schema.data_disattivazione)
    val dateStdFlow = r.getAs[String](D01Schema.data_prest)
    val segnMisSost = r.getAs[String](D01Schema.segn_mis)
    val segnConv = r.getAs[String](D01Schema.segn_conv)
    val letTotPrel = r.getAs[String](D01Schema.let_tot_prel)
    val letTotConv = r.getAs[String](D01Schema.let_tot_conv)
    var esito = r.getAs[String](D01Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    D01(
      service = r.getAs[String](D01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](D01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](D01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](D01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](D01Schema.matr_conv)),
      localFile = Option(r.getAs[String](D01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](D01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](D01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](D01Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None))
  }
}

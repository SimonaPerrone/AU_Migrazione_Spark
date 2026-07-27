package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{A01, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A01Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class A01DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getA01ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    A01Schema.cod_servizio,
    A01Schema.cod_pdr,
    A01Schema.data_attivazione,
    A01Schema.data_prest,
    A01Schema.tipo_lettura,
    A01Schema.segn_mis,
    A01Schema.segn_conv,
    A01Schema.let_tot_prel,
    A01Schema.let_tot_conv,
    A01Schema.matr_mis,
    A01Schema.matr_conv,
    A01Schema.local_file,
    A01Schema.d_caricamento,
    A01Schema.piva_distr,
    A01Schema.piva_utente,
    A01Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1')) ||
    (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](A01Schema.data_attivazione)
    val dateStdFlow = r.getAs[String](A01Schema.data_prest)
    val segnMisSost = r.getAs[String](A01Schema.segn_mis)
    val segnConv = r.getAs[String](A01Schema.segn_conv)
    val letTotPrel = r.getAs[String](A01Schema.let_tot_prel)
    val letTotConv = r.getAs[String](A01Schema.let_tot_conv)
    var esito = r.getAs[String](A01Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    A01(
      service = r.getAs[String](A01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](A01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](A01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](A01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A01Schema.matr_conv)),
      localFile = Option(r.getAs[String](A01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](A01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A01Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}

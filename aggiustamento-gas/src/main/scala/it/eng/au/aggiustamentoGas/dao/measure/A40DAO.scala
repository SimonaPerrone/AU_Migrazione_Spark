package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{A40, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A40Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class A40DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getA40ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    A40Schema.cod_servizio,
    A40Schema.cod_pdr,
    A40Schema.data_attivazione,
    A40Schema.data_prest,
    A40Schema.tipo_lettura,
    A40Schema.segn_mis,
    A40Schema.segn_conv,
    A40Schema.let_tot_prel,
    A40Schema.let_tot_conv,
    A40Schema.matr_mis,
    A40Schema.matr_conv,
    A40Schema.local_file,
    A40Schema.d_caricamento,
    A40Schema.piva_distr,
    A40Schema.piva_utente,
    A40Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1')) ||
    (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](A40Schema.data_attivazione)
    val dateStdFlow = r.getAs[String](A40Schema.data_prest)
    val segnMis = r.getAs[String](A40Schema.segn_mis)
    val segnConv = r.getAs[String](A40Schema.segn_conv)
    val letTotPrel = r.getAs[String](A40Schema.let_tot_prel)
    val letTotConv = r.getAs[String](A40Schema.let_tot_conv)
    var esito = r.getAs[String](A40Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    A40(
      service = r.getAs[String](A40Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](A40Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](A40Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMis),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](A40Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A40Schema.matr_conv)),
      localFile = Option(r.getAs[String](A40Schema.local_file)),
      pivaDistr = Option(r.getAs[String](A40Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A40Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A40Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}

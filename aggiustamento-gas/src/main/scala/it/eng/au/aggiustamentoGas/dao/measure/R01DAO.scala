package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, R01}
import it.eng.au.aggiustamentoGas.schema.measure.R01Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class R01DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getR01ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    R01Schema.cod_servizio,
    R01Schema.cod_pdr,
    R01Schema.data_attivazione,
    R01Schema.data_prest,
    R01Schema.tipo_lettura,
    R01Schema.segn_mis,
    R01Schema.segn_conv,
    R01Schema.let_tot_prel,
    R01Schema.let_tot_conv,
    R01Schema.matr_mis,
    R01Schema.matr_conv,
    R01Schema.local_file,
    R01Schema.d_caricamento,
    R01Schema.piva_distr,
    R01Schema.piva_utente,
    R01Schema.esito,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1')) || (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](R01Schema.data_attivazione)
    val dateStdFlow = r.getAs[String](R01Schema.data_prest)
    val segnMisSost = r.getAs[String](R01Schema.segn_mis)
    val segnConv = r.getAs[String](R01Schema.segn_conv)
    val letTotPrel = r.getAs[String](R01Schema.let_tot_prel)
    val letTotConv = r.getAs[String](R01Schema.let_tot_conv)
    var esito = r.getAs[String](R01Schema.esito)
    esito = if (Try(esito.trim.equalsIgnoreCase("null")).getOrElse(true)) "" else esito //sometimes null are encoded as strings

    R01(
      service = r.getAs[String](R01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](R01Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](R01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](R01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](R01Schema.matr_conv)),
      localFile = Option(r.getAs[String](R01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](R01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](R01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](R01Schema.d_caricamento), MeasureDAO.dateLoadFormatter),
      outcome = Try(Option(esito).map(_.trim.charAt(0))).getOrElse(None)
    )
  }
}

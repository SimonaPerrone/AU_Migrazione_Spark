package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{A02, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A02Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class A02DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getA02ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    A02Schema.cod_servizio,
    A02Schema.cod_pdr,
    A02Schema.data_sospensione,
    A02Schema.data_prest,
    A02Schema.tipo_lettura,
    A02Schema.segn_mis,
    A02Schema.segn_conv,
    A02Schema.let_tot_prel,
    A02Schema.let_tot_conv,
    A02Schema.matr_mis,
    A02Schema.matr_conv,
    A02Schema.local_file,
    A02Schema.d_caricamento,
    A02Schema.piva_distr,
    A02Schema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => f.ammissibilita.isEmpty || (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](A02Schema.data_sospensione)
    val dateStdFlow = r.getAs[String](A02Schema.data_prest)
    val segnMisSost = r.getAs[String](A02Schema.segn_mis)
    val segnConv = r.getAs[String](A02Schema.segn_conv)
    val letTotPrel = r.getAs[String](A02Schema.let_tot_prel)
    val letTotConv = r.getAs[String](A02Schema.let_tot_conv)

    A02(
      service = r.getAs[String](A02Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](A02Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](A02Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateStdFlow, dateOldFlow), //date
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](A02Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A02Schema.matr_conv)),
      localFile = Option(r.getAs[String](A02Schema.local_file)),
      pivaDistr = Option(r.getAs[String](A02Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A02Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A02Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

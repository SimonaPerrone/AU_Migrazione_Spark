package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, V02}
import it.eng.au.aggiustamentoGas.schema.measure.V02Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class V02DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getV02ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    V02Schema.cod_servizio,
    V02Schema.cod_pdr,
    V02Schema.data_prest,
    V02Schema.let_tot_prel,
    V02Schema.let_tot_conv,
    V02Schema.matr_mis,
    V02Schema.matr_conv,
    V02Schema.local_file,
    V02Schema.d_caricamento,
    V02Schema.piva_distr,
    V02Schema.piva_utente,
    V02Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    V02(
      service = r.getAs[String](V02Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](V02Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](V02Schema.data_prest)),
      readType = Try(Option(r.getAs[String](V02Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](V02Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](V02Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](V02Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](V02Schema.matr_conv)),
      localFile = Option(r.getAs[String](V02Schema.local_file)),
      pivaDistr = Option(r.getAs[String](V02Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](V02Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](V02Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

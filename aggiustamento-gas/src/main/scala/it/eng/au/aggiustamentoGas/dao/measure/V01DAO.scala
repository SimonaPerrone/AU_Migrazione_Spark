package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, V01}
import it.eng.au.aggiustamentoGas.schema.measure.V01Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class V01DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getV01ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    V01Schema.cod_servizio,
    V01Schema.cod_pdr,
    V01Schema.data_prest,
    V01Schema.let_tot_prel,
    V01Schema.let_tot_conv,
    V01Schema.matr_mis,
    V01Schema.matr_conv,
    V01Schema.local_file,
    V01Schema.d_caricamento,
    V01Schema.piva_distr,
    V01Schema.piva_utente,
    V01Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    V01(
      service = r.getAs[String](V01Schema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](V01Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](V01Schema.data_prest)),
      readType = Try(Option(r.getAs[String](V01Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](V01Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](V01Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](V01Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](V01Schema.matr_conv)),
      localFile = Option(r.getAs[String](V01Schema.local_file)),
      pivaDistr = Option(r.getAs[String](V01Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](V01Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](V01Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

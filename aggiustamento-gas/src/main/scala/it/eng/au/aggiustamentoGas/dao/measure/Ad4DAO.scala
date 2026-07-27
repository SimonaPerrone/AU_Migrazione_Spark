package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad4, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad4Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad4DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd4ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad4Schema.cod_flusso,
    Ad4Schema.cod_pdr,
    Ad4Schema.data_prest,
    Ad4Schema.let_tot_prel,
    Ad4Schema.let_tot_conv,
    Ad4Schema.matr_mis,
    Ad4Schema.matr_conv,
    Ad4Schema.local_file,
    Ad4Schema.d_caricamento,
    Ad4Schema.piva_distr,
    Ad4Schema.piva_utente,
    Ad4Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad4(
      service = r.getAs[String](Ad4Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad4Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad4Schema.data_prest)),
      readType = Try(Option(r.getAs[String](Ad4Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Ad4Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad4Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad4Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad4Schema.matr_conv)),
      localFile = Option(r.getAs[String](Ad4Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad4Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad4Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad4Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

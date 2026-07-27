package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad5, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad5Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad5DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd5ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad5Schema.cod_flusso,
    Ad5Schema.cod_pdr,
    Ad5Schema.data_prest,
    Ad5Schema.let_tot_prel,
    Ad5Schema.let_tot_conv,
    Ad5Schema.matr_mis,
    Ad5Schema.matr_conv,
    Ad5Schema.local_file,
    Ad5Schema.d_caricamento,
    Ad5Schema.piva_distr,
    Ad5Schema.piva_utente,
    Ad5Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad5(
      service = r.getAs[String](Ad5Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad5Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad5Schema.data_prest)),
      readType = Try(Option(r.getAs[String](Ad5Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Ad5Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad5Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad5Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad5Schema.matr_conv)),
      localFile = Option(r.getAs[String](Ad5Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad5Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad5Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad5Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad4r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad4rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad4rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd4rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad4rSchema.cod_flusso,
    Ad4rSchema.cod_pdr,
    Ad4rSchema.data_prest,
    Ad4rSchema.let_tot_prel,
    Ad4rSchema.let_tot_conv,
    Ad4rSchema.matr_mis,
    Ad4rSchema.matr_conv,
    Ad4rSchema.local_file,
    Ad4rSchema.d_caricamento,
    Ad4rSchema.piva_distr,
    Ad4rSchema.piva_utente,
    Ad4rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad4r(
      service = r.getAs[String](Ad4rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad4rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad4rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](Ad4rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad4rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad4rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad4rSchema.matr_conv)),
      localFile = Option(r.getAs[String](Ad4rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad4rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad4rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](Ad4rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad4rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

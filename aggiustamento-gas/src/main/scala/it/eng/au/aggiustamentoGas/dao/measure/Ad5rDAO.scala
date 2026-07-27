package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad5r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad5rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad5rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd5rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad5rSchema.cod_flusso,
    Ad5rSchema.cod_pdr,
    Ad5rSchema.data_prest,
    Ad5rSchema.let_tot_prel,
    Ad5rSchema.let_tot_conv,
    Ad5rSchema.matr_mis,
    Ad5rSchema.matr_conv,
    Ad5rSchema.local_file,
    Ad5rSchema.d_caricamento,
    Ad5rSchema.piva_distr,
    Ad5rSchema.piva_utente,
    Ad5rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad5r(
      service = r.getAs[String](Ad5rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad5rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad5rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](Ad5rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad5rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad5rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad5rSchema.matr_conv)),
      localFile = Option(r.getAs[String](Ad5rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad5rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad5rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](Ad5rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad5rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

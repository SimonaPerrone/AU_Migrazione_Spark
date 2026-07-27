package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, S02r}
import it.eng.au.aggiustamentoGas.schema.measure.S02rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class S02rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getS02rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    S02rSchema.cod_flusso,
    S02rSchema.cod_pdr,
    S02rSchema.data_prest,
    S02rSchema.let_tot_prel,
    S02rSchema.let_tot_conv,
    S02rSchema.matr_mis,
    S02rSchema.matr_conv,
    S02rSchema.local_file,
    S02rSchema.d_caricamento,
    S02rSchema.piva_distr,
    S02rSchema.piva_utente,
    S02rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    S02r(
      service = r.getAs[String](S02rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](S02rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](S02rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](S02rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](S02rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](S02rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](S02rSchema.matr_conv)),
      localFile = Option(r.getAs[String](S02rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](S02rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](S02rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](S02rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](S02rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

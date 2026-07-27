package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, S40r}
import it.eng.au.aggiustamentoGas.schema.measure.S40rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class S40rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getS40rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    S40rSchema.cod_flusso,
    S40rSchema.cod_pdr,
    S40rSchema.data_prest,
    S40rSchema.let_tot_prel,
    S40rSchema.let_tot_conv,
    S40rSchema.matr_mis,
    S40rSchema.matr_conv,
    S40rSchema.local_file,
    S40rSchema.d_caricamento,
    S40rSchema.piva_distr,
    S40rSchema.piva_utente,
    S40rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    S40r(
      service = r.getAs[String](S40rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](S40rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](S40rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](S40rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](S40rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](S40rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](S40rSchema.matr_conv)),
      localFile = Option(r.getAs[String](S40rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](S40rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](S40rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](S40rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](S40rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

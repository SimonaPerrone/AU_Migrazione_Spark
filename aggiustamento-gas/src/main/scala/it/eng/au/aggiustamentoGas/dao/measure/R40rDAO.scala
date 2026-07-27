package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, R40r}
import it.eng.au.aggiustamentoGas.schema.measure.R40rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class R40rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getR40rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    R40rSchema.cod_flusso,
    R40rSchema.cod_pdr,
    R40rSchema.data_prest,
    R40rSchema.let_tot_prel,
    R40rSchema.let_tot_conv,
    R40rSchema.matr_mis,
    R40rSchema.matr_conv,
    R40rSchema.local_file,
    R40rSchema.d_caricamento,
    R40rSchema.piva_distr,
    R40rSchema.piva_utente,
    R40rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    R40r(
      service = r.getAs[String](R40rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](R40rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](R40rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](R40rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](R40rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](R40rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](R40rSchema.matr_conv)),
      localFile = Option(r.getAs[String](R40rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](R40rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](R40rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](R40rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](R40rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

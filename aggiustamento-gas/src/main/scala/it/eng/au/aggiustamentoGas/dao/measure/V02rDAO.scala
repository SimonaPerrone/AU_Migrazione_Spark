package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, V02r}
import it.eng.au.aggiustamentoGas.schema.measure.V02rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class V02rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getV02rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    V02rSchema.cod_flusso,
    V02rSchema.cod_pdr,
    V02rSchema.data_prest,
    V02rSchema.let_tot_prel,
    V02rSchema.let_tot_conv,
    V02rSchema.matr_mis,
    V02rSchema.matr_conv,
    V02rSchema.local_file,
    V02rSchema.d_caricamento,
    V02rSchema.piva_distr,
    V02rSchema.piva_utente,
    V02rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    V02r(
      service = r.getAs[String](V02rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](V02rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](V02rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](V02rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](V02rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](V02rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](V02rSchema.matr_conv)),
      localFile = Option(r.getAs[String](V02rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](V02rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](V02rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](V02rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](V02rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

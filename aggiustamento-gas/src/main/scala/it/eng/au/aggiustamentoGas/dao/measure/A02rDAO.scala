package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{A02r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.A02rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class A02rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getA02rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    A02rSchema.cod_flusso,
    A02rSchema.cod_pdr,
    A02rSchema.data_prest,
    A02rSchema.let_tot_prel,
    A02rSchema.let_tot_conv,
    A02rSchema.matr_mis,
    A02rSchema.matr_conv,
    A02rSchema.local_file,
    A02rSchema.d_caricamento,
    A02rSchema.piva_distr,
    A02rSchema.piva_utente,
    A02rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    A02r(
      service = r.getAs[String](A02rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](A02rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](A02rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](A02rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](A02rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](A02rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](A02rSchema.matr_conv)),
      localFile = Option(r.getAs[String](A02rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](A02rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](A02rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](A02rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](A02rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

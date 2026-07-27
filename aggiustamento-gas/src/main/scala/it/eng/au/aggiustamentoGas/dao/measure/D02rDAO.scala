package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{D02r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.D02rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class D02rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getD02rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    D02rSchema.cod_flusso,
    D02rSchema.cod_pdr,
    D02rSchema.data_prest,
    D02rSchema.let_tot_prel,
    D02rSchema.let_tot_conv,
    D02rSchema.matr_mis,
    D02rSchema.matr_conv,
    D02rSchema.local_file,
    D02rSchema.d_caricamento,
    D02rSchema.piva_distr,
    D02rSchema.piva_utente,
    D02rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    D02r(
      service = r.getAs[String](D02rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](D02rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](D02rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](D02rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](D02rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](D02rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](D02rSchema.matr_conv)),
      localFile = Option(r.getAs[String](D02rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](D02rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](D02rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](D02rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](D02rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

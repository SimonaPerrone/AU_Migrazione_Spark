package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, M01r}
import it.eng.au.aggiustamentoGas.schema.measure.M01rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class M01rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getM01rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    M01rSchema.cod_flusso,
    M01rSchema.cod_pdr,
    M01rSchema.data_prest,
    M01rSchema.let_tot_prel,
    M01rSchema.let_tot_conv,
    M01rSchema.matr_mis,
    M01rSchema.matr_conv,
    M01rSchema.local_file,
    M01rSchema.d_caricamento,
    M01rSchema.piva_distr,
    M01rSchema.piva_utente,
    M01rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    M01r(
      service = r.getAs[String](M01rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](M01rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](M01rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](M01rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](M01rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](M01rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](M01rSchema.matr_conv)),
      localFile = Option(r.getAs[String](M01rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](M01rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](M01rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](M01rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](M01rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

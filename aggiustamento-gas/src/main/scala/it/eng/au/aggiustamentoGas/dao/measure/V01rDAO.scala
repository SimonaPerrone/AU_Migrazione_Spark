package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, V01r}
import it.eng.au.aggiustamentoGas.schema.measure.V01rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class V01rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getV01rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    V01rSchema.cod_flusso,
    V01rSchema.cod_pdr,
    V01rSchema.data_prest,
    V01rSchema.let_tot_prel,
    V01rSchema.let_tot_conv,
    V01rSchema.matr_mis,
    V01rSchema.matr_conv,
    V01rSchema.local_file,
    V01rSchema.d_caricamento,
    V01rSchema.piva_distr,
    V01rSchema.piva_utente,
    V01rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    V01r(
      service = r.getAs[String](V01rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](V01rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](V01rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](V01rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](V01rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](V01rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](V01rSchema.matr_conv)),
      localFile = Option(r.getAs[String](V01rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](V01rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](V01rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](V01rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](V01rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

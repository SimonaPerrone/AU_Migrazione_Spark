package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{D01r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.D01rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class D01rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getD01rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    D01rSchema.cod_flusso,
    D01rSchema.cod_pdr,
    D01rSchema.data_prest,
    D01rSchema.let_tot_prel,
    D01rSchema.let_tot_conv,
    D01rSchema.matr_mis,
    D01rSchema.matr_conv,
    D01rSchema.local_file,
    D01rSchema.d_caricamento,
    D01rSchema.piva_distr,
    D01rSchema.piva_utente,
    D01rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    D01r(
      service = r.getAs[String](D01rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](D01rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](D01rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](D01rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](D01rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](D01rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](D01rSchema.matr_conv)),
      localFile = Option(r.getAs[String](D01rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](D01rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](D01rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](D01rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](D01rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

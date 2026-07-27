package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, Sm1r}
import it.eng.au.aggiustamentoGas.schema.measure.Sm1rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Sm1rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getSm1rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Sm1rSchema.cod_flusso,
    Sm1rSchema.cod_pdr,
    Sm1rSchema.data_prest,
    Sm1rSchema.let_tot_prel,
    Sm1rSchema.let_tot_conv,
    Sm1rSchema.matr_mis,
    Sm1rSchema.matr_conv,
    Sm1rSchema.local_file,
    Sm1rSchema.d_caricamento,
    Sm1rSchema.piva_distr,
    Sm1rSchema.piva_utente,
    Sm1rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Sm1r(
      service = r.getAs[String](Sm1rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Sm1rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Sm1rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](Sm1rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Sm1rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Sm1rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Sm1rSchema.matr_conv)),
      localFile = Option(r.getAs[String](Sm1rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](Sm1rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Sm1rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](Sm1rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Sm1rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rmv}
import it.eng.au.aggiustamentoGas.schema.measure.RmvSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class RmvDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getRmvParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    RmvSchema.cod_servizio,
    RmvSchema.cod_pdr,
    RmvSchema.data_comp,
    RmvSchema.data_prest,
    RmvSchema.let_tot_prel,
    RmvSchema.let_tot_conv,
    RmvSchema.matr_mis,
    RmvSchema.matr_conv,
    RmvSchema.local_file,
    RmvSchema.d_caricamento,
    RmvSchema.piva_distr,
    RmvSchema.piva_utente,
    RmvSchema.mot_rett_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    val data_comp = r.getAs[String](RmvSchema.data_comp)
    val data_prest = r.getAs[String](RmvSchema.data_prest)

    Rmv(
      service = r.getAs[String](RmvSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](RmvSchema.cod_pdr),
      date = MeasureDAO.getDate(data_prest, data_comp),
      measure = Try(Option(r.getAs[String](RmvSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](RmvSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](RmvSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](RmvSchema.matr_conv)),
      localFile = Option(r.getAs[String](RmvSchema.local_file)),
      pivaDistr = Option(r.getAs[String](RmvSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](RmvSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](RmvSchema.d_caricamento), MeasureDAO.dateLoadFormatter),
      motivation = Try(Option(r.getAs[String](RmvSchema.mot_rett_lett).toInt)).getOrElse(None)
    )
  }
}

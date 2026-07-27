package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, RslDAO}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rsl}
import it.eng.au.aggiustamentoGas.schema.measure.RslSchema
import org.apache.spark.sql.Row

import scala.util.Try

class RslDAOSbg extends RslDAO {
  override val columns: List[String] = List(
    RslSchema.cod_servizio,
    RslSchema.cod_pdr,
    RslSchema.data_comp,
    RslSchema.data_prest,
    RslSchema.let_tot_prel,
    RslSchema.let_tot_conv,
    RslSchema.matr_mis,
    RslSchema.matr_conv,
    RslSchema.local_file,
    RslSchema.d_caricamento,
    RslSchema.piva_distr,
    RslSchema.piva_utente,
    RslSchema.mot_rett_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

//  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Rsl(
      service = r.getAs[String](RslSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](RslSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](RslSchema.data_prest)),
      measure = Try(Option(r.getAs[String](RslSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](RslSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](RslSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](RslSchema.matr_conv)),
      localFile = Option(r.getAs[String](RslSchema.local_file)),
      pivaDistr = Option(r.getAs[String](RslSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](RslSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](RslSchema.d_caricamento), MeasureDAO.dateLoadFormatter),
      motivation = Try(Option(r.getAs[String](RslSchema.mot_rett_lett).toInt)).getOrElse(None)
    )
  }
}

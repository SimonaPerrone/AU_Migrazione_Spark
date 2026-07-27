package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rgl}
import it.eng.au.aggiustamentoGas.schema.measure.RglSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class RglDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getRglParquetPath
  override val partitionDateColumn: Column = meseCompColumn
  override val columns: List[String] = List(
    RglSchema.cod_servizio,
    RglSchema.cod_pdr,
    RglSchema.mot_rett_lett,
    RglSchema.data_racc,
    RglSchema.let_tot_prel,
    RglSchema.let_tot_conv,
    RglSchema.matr_mis,
    RglSchema.matr_conv,
    RglSchema.matr_mis_giornaliere,
    RglSchema.matr_conv_giornaliere,
    RglSchema.local_file,
    RglSchema.piva_distr,
    RglSchema.piva_utente,
    RglSchema.d_caricamento,
    MESE_COMP_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && Set(1, 2, 3, 4, 5, 6).contains(f.motivation.getOrElse(-1))) ||
    (f.ammissibilita.isDefined && Set(1, 2, 3, 4, 5, 6, 7).contains(f.motivation.getOrElse(-1)))

  override val mapFunc: Row => Flow = (r: Row) => {
    Rgl(
      service = r.getAs[String](RglSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(RglSchema.cod_pdr).toString,
      date = parseDateToOption(r.getAs[String](RglSchema.data_racc)),
      measure = Try(Option(r.getAs[String](RglSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](RglSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = getStringField(r.getAs[String](RglSchema.matr_mis_giornaliere), r.getAs[String](RglSchema.matr_mis)),
      serialNumberConv = getStringField(r.getAs[String](RglSchema.matr_conv_giornaliere), r.getAs[String](RglSchema.matr_conv)),
      motivation = Option(r.getAs[String](RglSchema.mot_rett_lett).toInt),
      localFile = Option(r.getAs[String](RglSchema.local_file)),
      pivaDistr = Option(r.getAs[String](RglSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](RglSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](RglSchema.d_caricamento), dateLoadFormatter)
    )
  }
}

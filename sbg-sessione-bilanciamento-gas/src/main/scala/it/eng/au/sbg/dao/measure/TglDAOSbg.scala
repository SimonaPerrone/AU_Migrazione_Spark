package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{MESE_COMP_COL_NAME, dateLoadFormatter, getStringField, parseDateToOption}
import it.eng.au.aggiustamentoGas.dao.measure.TglDAO
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tgl}
import it.eng.au.aggiustamentoGas.schema.measure.TglSchema
import org.apache.spark.sql.Row

import scala.util.Try

class TglDAOSbg extends TglDAO {
  override val columns: List[String] = List(
    TglSchema.cod_servizio,
    TglSchema.cod_pdr,
    TglSchema.data_comp,
    TglSchema.let_tot_prel,
    TglSchema.let_tot_conv,
    TglSchema.matr_mis,
    TglSchema.matr_conv,
    TglSchema.matr_mis_giornaliere,
    TglSchema.matr_conv_giornaliere,
    TglSchema.tipo_lettura,
    TglSchema.local_file,
    TglSchema.d_caricamento,
    TglSchema.piva_distr,
    TglSchema.piva_utente,
    MESE_COMP_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Tgl(
      service = r.getAs[String](TglSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(TglSchema.cod_pdr).toString,
      readType = Try(Option(r.getAs[String](TglSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = parseDateToOption(r.getAs[String](TglSchema.data_comp)),
      measure = Try(Option(r.getAs[String](TglSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](TglSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = getStringField(r.getAs[String](TglSchema.matr_mis_giornaliere), r.getAs[String](TglSchema.matr_mis)),
      serialNumberConv = getStringField(r.getAs[String](TglSchema.matr_conv_giornaliere), r.getAs[String](TglSchema.matr_conv)),
      localFile = Option(r.getAs[String](TglSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TglSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TglSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](TglSchema.d_caricamento), dateLoadFormatter)
    )
  }
}

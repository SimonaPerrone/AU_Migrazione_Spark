package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn}
import it.eng.au.aggiustamentoGas.model.measure.{Fdd, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.FddSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class FddDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getDefParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    FddSchema.cod_servizio,
    FddSchema.cod_pdr,
    FddSchema.data_prest,
    FddSchema.tipo_lettura,
    FddSchema.let_tot_prel,
    FddSchema.let_tot_conv,
    FddSchema.matr_mis,
    FddSchema.matr_conv,
    FddSchema.local_file,
    FddSchema.d_caricamento,
    FddSchema.piva_distr,
    FddSchema.piva_utente,
    FddSchema.data_prestdata_mis_eff,
    FddSchema.let_tot_prelsegn_mis_eff,
    FddSchema.let_tot_convsegn_conv_eff,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dataPrest = r.getAs[String](FddSchema.data_prest)
    val dataMisEff = r.getAs[String](FddSchema.data_prestdata_mis_eff)
    val letTotPrel = r.getAs[String](FddSchema.let_tot_prel)
    val segnMisEff = r.getAs[String](FddSchema.let_tot_prelsegn_mis_eff)
    val letTotConv = r.getAs[String](FddSchema.let_tot_conv)
    val segnConvEff = r.getAs[String](FddSchema.let_tot_convsegn_conv_eff)

    Fdd(
      service = r.getAs[String](FddSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](FddSchema.cod_pdr),
      readType = Try(Option(r.getAs[String](FddSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dataMisEff, dataPrest),
      measure = MeasureDAO.getDoubleField(segnMisEff, letTotPrel),
      converted = MeasureDAO.getDoubleField(segnConvEff, letTotConv),
      serialNumberMis = Option(r.getAs[String](FddSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](FddSchema.matr_conv)),
      localFile = Option(r.getAs[String](FddSchema.local_file)),
      pivaDistr = Option(r.getAs[String](FddSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](FddSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](FddSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

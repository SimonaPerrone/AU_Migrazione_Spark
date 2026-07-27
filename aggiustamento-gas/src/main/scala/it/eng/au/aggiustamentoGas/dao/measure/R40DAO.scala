package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, R40}
import it.eng.au.aggiustamentoGas.schema.measure.R40Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class R40DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getR40ParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    R40Schema.cod_flusso,
    R40Schema.cod_pdr,
    R40Schema.data_prest,
    R40Schema.tipo_lettura,
    R40Schema.segn_mis_eff,
    R40Schema.segn_conv_eff,
    R40Schema.let_tot_prel,
    R40Schema.let_tot_conv,
    R40Schema.matr_mis,
    R40Schema.matr_conv,
    R40Schema.local_file,
    R40Schema.d_caricamento,
    R40Schema.piva_distr,
    R40Schema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val segnMisEff = r.getAs[String](R40Schema.segn_mis_eff)
    val segnConvEff = r.getAs[String](R40Schema.segn_conv_eff)
    val letTotPrel = r.getAs[String](R40Schema.let_tot_prel)
    val letTotConv = r.getAs[String](R40Schema.let_tot_conv)

    R40(
      service = r.getAs[String](R40Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](R40Schema.cod_pdr),
      readType = Try(Option(r.getAs[String](R40Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.parseDateToOption(r.getAs[String](R40Schema.data_prest)),
      measure = getDoubleField(letTotPrel, segnMisEff),
      converted = getDoubleField(letTotConv, segnConvEff),
      serialNumberMis = Option(r.getAs[String](R40Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](R40Schema.matr_conv)),
      localFile = Option(r.getAs[String](R40Schema.local_file)),
      pivaDistr = Option(r.getAs[String](R40Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](R40Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](R40Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

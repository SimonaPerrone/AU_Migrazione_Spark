package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, getDoubleField}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Tmv}
import it.eng.au.aggiustamentoGas.schema.measure.TmvSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class TmvDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getTmvParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    TmvSchema.cod_servizio,
    TmvSchema.cod_pdr,
    TmvSchema.data_att_contr,
    TmvSchema.data_prest,
    TmvSchema.tipo_lettura,
    TmvSchema.segn_mis_sost,
    TmvSchema.segn_conv,
    TmvSchema.let_tot_prel,
    TmvSchema.let_tot_conv,
    TmvSchema.matr_mis,
    TmvSchema.matr_conv,
    TmvSchema.local_file,
    TmvSchema.d_caricamento,
    TmvSchema.piva_distr,
    TmvSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && Set('E', 'S').contains(f.readType.getOrElse('-'))) ||
    (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dateOldFlow = r.getAs[String](TmvSchema.data_att_contr)
    val dateStdFlow = r.getAs[String](TmvSchema.data_prest)
    val segnMisSost = r.getAs[String](TmvSchema.segn_mis_sost)
    val segnConv = r.getAs[String](TmvSchema.segn_conv)
    val letTotPrel = r.getAs[String](TmvSchema.let_tot_prel)
    val letTotConv = r.getAs[String](TmvSchema.let_tot_conv)

    Tmv(
      service = r.getAs[String](TmvSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](TmvSchema.cod_pdr),
      readType = Try(Option(r.getAs[String](TmvSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.getDate(dateOldFlow, dateStdFlow), //dateOldFlow has precedence in this case (Mapping Campi Parquet_v.1.1.xlsx)
      measure = getDoubleField(letTotPrel, segnMisSost),
      converted = getDoubleField(letTotConv, segnConv),
      serialNumberMis = Option(r.getAs[String](TmvSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](TmvSchema.matr_conv)),
      localFile = Option(r.getAs[String](TmvSchema.local_file)),
      pivaDistr = Option(r.getAs[String](TmvSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](TmvSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](TmvSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

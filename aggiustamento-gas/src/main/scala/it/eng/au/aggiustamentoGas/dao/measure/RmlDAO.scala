package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rml}
import it.eng.au.aggiustamentoGas.schema.measure.RmlSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class RmlDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getRmlParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    RmlSchema.cod_servizio,
    RmlSchema.cod_pdr,
    RmlSchema.mot_rett_lett,
    RmlSchema.freq_let,
    RmlSchema.data_racc,
    RmlSchema.let_tot_prel,
    RmlSchema.let_tot_conv,
    RmlSchema.matr_mis,
    RmlSchema.matr_conv,
    RmlSchema.local_file,
    RmlSchema.tipo_rettifica,
    RmlSchema.d_caricamento,
    RmlSchema.piva_distr,
    RmlSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5, 6).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Rml(
      service = r.getAs[String](RmlSchema.cod_servizio).toUpperCase,
      pdr = r.getAs(RmlSchema.cod_pdr).toString,
      readType = Try(Option(r.getAs[String](RmlSchema.mot_rett_lett)).map(_ (0))).getOrElse(None),
      date = parseDateToOption(r.getAs[String](RmlSchema.data_racc)),
      measure = Try(Option(r.getAs[String](RmlSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](RmlSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](RmlSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](RmlSchema.matr_conv)),
      motivation = Option(r.getAs[String](RmlSchema.mot_rett_lett).toInt),
      freqLet = Try(Option(r.getAs[String](RmlSchema.freq_let)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](RmlSchema.local_file)),
      pivaDistr = Option(r.getAs[String](RmlSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](RmlSchema.piva_utente)),
      tipoRettifica = Option(r.getAs[String](RmlSchema.tipo_rettifica)),
      dataCaricamento = parseDateToOption(r.getAs[String](RmlSchema.d_caricamento), dateLoadFormatter)
    )
  }
}

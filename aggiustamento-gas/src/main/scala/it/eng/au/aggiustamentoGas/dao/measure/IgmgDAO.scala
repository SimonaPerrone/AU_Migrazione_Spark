package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.controller.CoefficientController
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO._
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre}
import it.eng.au.aggiustamentoGas.schema.measure.IgmgSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class IgmgDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getIgmgParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    IgmgSchema.cod_flusso,
    IgmgSchema.cod_pdr,
    IgmgSchema.data_misura,
    IgmgSchema.let_misuratore_pre_int,
    IgmgSchema.let_correttore_pre_int,
    IgmgSchema.matr_mis_pre_int,
    IgmgSchema.matr_conv_pre_int,
    IgmgSchema.tipo_let,
    IgmgSchema.coeff_corr_pre_int,
    IgmgSchema.let_misuratore_post_int,
    IgmgSchema.let_correttore_post_int,
    IgmgSchema.matr_mis_post_int,
    IgmgSchema.matr_conv_post_int,
    IgmgSchema.coeff_corr_post_int,
    IgmgSchema.local_file,
    IgmgSchema.cau_int_mis,
    IgmgSchema.cau_int_cor,
    IgmgSchema.d_caricamento,
    IgmgSchema.piva_distr,
    IgmgSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S').contains(f.readType.getOrElse('-')) &&
    (f.asInstanceOf[Igmg].cau_int_cor.isDefined || f.asInstanceOf[Igmg].cau_int_mis.isDefined)

  override val mapFunc: Row => Flow = (r: Row) => {
    val igmgPre = IgmgPre(
      service = r.getAs[String](IgmgSchema.cod_flusso).toUpperCase + "PRE",
      pdr = r.getAs[String](IgmgSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmgSchema.data_misura)),
      readType = Try(Option(r.getAs[String](IgmgSchema.tipo_let)).map(_ (0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](IgmgSchema.let_misuratore_pre_int).toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](IgmgSchema.let_correttore_pre_int)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](IgmgSchema.matr_mis_pre_int)),
      serialNumberConv = Option(r.getAs[String](IgmgSchema.matr_conv_pre_int)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](IgmgSchema.coeff_corr_pre_int)).map(_.toDouble)).getOrElse(None)),
      cau_int_mis = Try(Option(r.getAs[String](IgmgSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmgSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmgSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmgSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmgSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmgSchema.d_caricamento), dateLoadFormatter)
    )

    val igmgPost = IgmgPost(
      service = r.getAs[String](IgmgSchema.cod_flusso).toUpperCase + "POST",
      pdr = r.getAs[String](IgmgSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmgSchema.data_misura)),
      readType = Try(Option(r.getAs[String](IgmgSchema.tipo_let)).map(_ (0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](IgmgSchema.let_misuratore_post_int)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](IgmgSchema.let_correttore_post_int)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](IgmgSchema.matr_mis_post_int)),
      serialNumberConv = Option(r.getAs[String](IgmgSchema.matr_conv_post_int)),
      coefCorr = CoefficientController.sanitizeCoefficient(Try(Option(r.getAs[String](IgmgSchema.coeff_corr_post_int)).map(_.toDouble)).getOrElse(None)),
      cau_int_mis = Try(Option(r.getAs[String](IgmgSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmgSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmgSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmgSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmgSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmgSchema.d_caricamento), dateLoadFormatter)
    )

    Igmg(
      service = r.getAs[String](IgmgSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](IgmgSchema.cod_pdr),
      date = parseDateToOption(r.getAs[String](IgmgSchema.data_misura)),
      readType = Try(Option(r.getAs[String](IgmgSchema.tipo_let)).map(_ (0))).getOrElse(None),
      cau_int_mis = Try(Option(r.getAs[String](IgmgSchema.cau_int_mis)).map(_.toInt)).getOrElse(None),
      cau_int_cor = Try(Option(r.getAs[String](IgmgSchema.cau_int_cor)).map(_.toInt)).getOrElse(None),
      localFile = Option(r.getAs[String](IgmgSchema.local_file)),
      pivaDistr = Option(r.getAs[String](IgmgSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](IgmgSchema.piva_utente)),
      dataCaricamento = parseDateToOption(r.getAs[String](IgmgSchema.d_caricamento), dateLoadFormatter),
      pre = igmgPre,
      post = igmgPost
    )
  }
}

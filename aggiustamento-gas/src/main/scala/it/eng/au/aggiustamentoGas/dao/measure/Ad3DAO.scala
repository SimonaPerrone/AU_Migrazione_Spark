package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad3, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad3Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad3DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd3ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad3Schema.cod_flusso,
    Ad3Schema.cod_pdr,
    Ad3Schema.data_prest,
    Ad3Schema.let_tot_prel,
    Ad3Schema.let_tot_conv,
    Ad3Schema.matr_mis,
    Ad3Schema.matr_conv,
    Ad3Schema.local_file,
    Ad3Schema.d_caricamento,
    Ad3Schema.piva_distr,
    Ad3Schema.piva_utente,
    Ad3Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad3(
      service = r.getAs[String](Ad3Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad3Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad3Schema.data_prest)),
      readType = Try(Option(r.getAs[String](Ad3Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Ad3Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad3Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad3Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad3Schema.matr_conv)),
      localFile = Option(r.getAs[String](Ad3Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad3Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad3Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad3Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

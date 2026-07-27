package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{D02, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.D02Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class D02DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getD02ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    D02Schema.cod_flusso,
    D02Schema.cod_pdr,
    D02Schema.data_prest,
    D02Schema.let_tot_prel,
    D02Schema.let_tot_conv,
    D02Schema.matr_mis,
    D02Schema.matr_conv,
    D02Schema.local_file,
    D02Schema.d_caricamento,
    D02Schema.piva_distr,
    D02Schema.piva_utente,
    D02Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => (f.ammissibilita.isEmpty && f.outcome == Some('1')) || (f.ammissibilita.isDefined && Set('E', 'S', 'A').contains(f.readType.getOrElse('-')))

  override val mapFunc: Row => Flow = (r: Row) => {
    D02(
      service = r.getAs[String](D02Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](D02Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](D02Schema.data_prest)),
      readType = Try(Option(r.getAs[String](D02Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](D02Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](D02Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](D02Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](D02Schema.matr_conv)),
      localFile = Option(r.getAs[String](D02Schema.local_file)),
      pivaDistr = Option(r.getAs[String](D02Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](D02Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](D02Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}

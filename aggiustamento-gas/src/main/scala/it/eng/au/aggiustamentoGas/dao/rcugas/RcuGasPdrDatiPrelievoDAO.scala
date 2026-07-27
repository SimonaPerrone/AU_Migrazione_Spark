package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasPdrDatiPrelievo
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasPdrDatiPrelievoSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

class RcuGasPdrDatiPrelievoDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasPdrDatiPrelievoPath
  override val columns: List[String] = List(
    RcuGasPdrDatiPrelievoSchema.n_id_pdr,
    RcuGasPdrDatiPrelievoSchema.n_prelievo_annuo,
    RcuGasPdrDatiPrelievoSchema.t_cod_profilo,
    RcuGasPdrDatiPrelievoSchema.t_anno
  )

  def get(): RDD[RcuGasPdrDatiPrelievo] = {
    readParquet.rdd.map(mapFunc)
  }

  val mapFunc: Row => RcuGasPdrDatiPrelievo = (r:Row) => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")

    RcuGasPdrDatiPrelievo(
      nIdPdr = r.getAs[String](RcuGasPdrDatiPrelievoSchema.n_id_pdr),
      nPrelievoAnnuo = Option(r.getAs[String](RcuGasPdrDatiPrelievoSchema.n_prelievo_annuo)).map(_.toDouble),
      tCodProfilo = Option(r.getAs[String](RcuGasPdrDatiPrelievoSchema.t_cod_profilo)),
      tAnno = r.getAs[String](RcuGasPdrDatiPrelievoSchema.t_anno)
    )
  }
}

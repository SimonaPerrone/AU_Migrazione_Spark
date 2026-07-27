package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.{RcuGasMassivoP, RcuGasVarTrattamentoP}
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarTrattamentoPSchema
import it.eng.au.aggiustamentoGas.utility.constants.{Treatment, TreatmentCalcMode}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.format.DateTimeFormat

import scala.util.{Failure, Success, Try}

/** Contiene il trattamento per un dato PdR */
class RcuGasVarTrattamentoPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasVarTrattamentoPath
  override val columns: List[String] = List(
    RcuGasVarTrattamentoPSchema.n_id_pdr,
    RcuGasVarTrattamentoPSchema.d_data_inizio,
    RcuGasVarTrattamentoPSchema.d_data_fine,
    RcuGasVarTrattamentoPSchema.t_trattamento_settlement
  )
  def get(rcuGasMassivo: RDD[RcuGasMassivoP], startFlow: String, endFlow: String, treatmentCalcMode: TreatmentCalcMode.Value = TreatmentCalcMode.rcugas): RDD[RcuGasVarTrattamentoP] = {
    if(treatmentCalcMode.equals(TreatmentCalcMode.rcugas))
      prepare(this.readParquet, startFlow, endFlow, rcuGasMassivo)
        .rdd
        .map(mapFunc)

    else {
      val emptyList: List[RcuGasVarTrattamentoP] = List()
      Environment.getSpark.sparkContext.parallelize(emptyList)
    }
  }

  def prepare(df: DataFrame, startFlow: String, endFlow: String, rcuGasMassivo: RDD[RcuGasMassivoP]): DataFrame = {

    val nIdPdr = "massivo_n_id_pdr"
    val tCodicePdr = "massivo_t_codice_pdr"

    val rcuMassivo = Environment.getSpark.sqlContext.createDataFrame(rcuGasMassivo.map(rcu => (rcu.nIdPdr, rcu.tCodicePdr)).distinct())
      .withColumnRenamed("_1", nIdPdr)
      .withColumnRenamed("_2", tCodicePdr)

    filterDfWithStartEndDate(df
      , RcuGasVarTrattamentoPSchema.d_data_inizio.toString
      , RcuGasVarTrattamentoPSchema.d_data_fine.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , startFlow
      , endFlow
      , "yyyyMM"
    )
      .withColumn(RcuGasVarTrattamentoPSchema.d_data_inizio, col(RcuGasVarTrattamentoPSchema.d_data_inizio).cast(StringType))
      .withColumn(RcuGasVarTrattamentoPSchema.d_data_fine, col(RcuGasVarTrattamentoPSchema.d_data_fine).cast(StringType))
      .join(rcuMassivo, col(RcuGasVarTrattamentoPSchema.n_id_pdr) === col(nIdPdr), "inner")
      .withColumn(RcuGasVarTrattamentoPSchema.n_id_pdr, col(tCodicePdr))
      .drop(nIdPdr, tCodicePdr)
      .select(
        col(RcuGasVarTrattamentoPSchema.n_id_pdr)
        , col(RcuGasVarTrattamentoPSchema.d_data_inizio)
        , col(RcuGasVarTrattamentoPSchema.d_data_fine)
        , col(RcuGasVarTrattamentoPSchema.t_trattamento_settlement)
      )
  }

  val mapFunc: Row => RcuGasVarTrattamentoP = r => {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarTrattamentoPSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01").toDateTimeAtStartOfDay
    }
    val endDate = Try(formatter.parseLocalDate(r.getAs[String](RcuGasVarTrattamentoPSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
      case Success(value) => value
      case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31").toDateTimeAtStartOfDay
    }

    RcuGasVarTrattamentoP(
      codicePdr = r.getAs[String](RcuGasVarTrattamentoPSchema.n_id_pdr),
      dataInizio = startDate,
      dataFine = endDate,
      tTrattamentoSettlement = Treatment.values.find(_.toString == r.getAs[String](RcuGasVarTrattamentoPSchema.t_trattamento_settlement)).getOrElse(Treatment.N)
    )
  }
}

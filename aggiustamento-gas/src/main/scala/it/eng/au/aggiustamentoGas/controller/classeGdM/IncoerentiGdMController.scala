package it.eng.au.aggiustamentoGas.controller.classeGdM

import it.eng.au.aggiustamentoGas.schema.agg.{DailyConsumptionAGGSBGSchema, DailyConsumptionIncoerentiGdMSchema}
import it.eng.au.aggiustamentoGas.dao.ClassiGruppiDiMisuraPortataRcugasDao
import it.eng.au.aggiustamentoGas.schema.tdg.TdgCoeffKSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{coalesce, col, datediff, lit, not, row_number, sum, udf, when}
import org.apache.spark.sql.types.IntegerType

class IncoerentiGdMController {

  // Colonne utilizzate per l'individuazione degli incoerenti GDM
  val isPdrAnomalousGDM: String = "isPdrAnomalousGdm"
  val isDayAnomalous: String = "isDayAnomalous"
  val portataMaxCoeffK: String = "portataMaxCoeffK"
  val dayOfMonth: String = "dayOfMonth"


  def findAnomalousDays(df: DataFrame, tdgCoeffDf: DataFrame): DataFrame = {
    //val thresholdAnomalousDays = Environment.getNumberOfDaysThresholdForGdm.toInt
    val windowByPdrInMonth = Window.partitionBy(col(DailyConsumptionAGGSBGSchema.pdr), col(DailyConsumptionAGGSBGSchema.annoMese))
    val windowByDates = Window.partitionBy(col(DailyConsumptionAGGSBGSchema.pdr), col(DailyConsumptionAGGSBGSchema.date)).orderBy(col("date_diff").asc)
    val anomalousDaysCount: String = "anomalous_days_count"
    val coefficientNonZero: String = "coefficient_non_zero"
    val consumoMensile: String = "consumo_mensile"
    val portataMensileC: String = "portata_mensile_c"
    val portataMensileK: String = "portata_mensile_k"

    val classiGdmToPortataMaxMap = ClassiGruppiDiMisuraPortataRcugasDao.getAsMap()
    val classiGdmToPortataMax: UserDefinedFunction = udf((classeGdm: String) => classiGdmToPortataMaxMap.get(classeGdm))

    val dfCoeff = df
      .withColumn(coefficientNonZero, when(col(DailyConsumptionAGGSBGSchema.coefficient)===0.0, lit(1.0)).otherwise(lit(col(DailyConsumptionAGGSBGSchema.coefficient))))

    val portataMassima = classiGdmToPortataMax(col(DailyConsumptionAGGSBGSchema.classeMisuratore)) * coalesce(col(coefficientNonZero), lit(1.0))

    val dfCoeffK = dfCoeff
      .join(tdgCoeffDf, col(DailyConsumptionAGGSBGSchema.pdr).equalTo(col(TdgCoeffKSchema.n_id_pdr)), "left")
      .withColumn("date_diff", datediff(col(DailyConsumptionAGGSBGSchema.date), col(TdgCoeffKSchema.d_data_inizio)))
      .withColumn("date_diff", when(col("date_diff") < 0, lit(100)).otherwise(lit(col("date_diff"))))
      .withColumn("row_number", row_number().over(windowByDates))
      .filter(col("row_number") === 1)
      .withColumn(TdgCoeffKSchema.n_val_k, when(col(TdgCoeffKSchema.n_val_k)===0.0, lit(1.0)).otherwise(lit(col(TdgCoeffKSchema.n_val_k))))
      .withColumn(portataMaxCoeffK, lit(classiGdmToPortataMax(col(DailyConsumptionAGGSBGSchema.classeMisuratore)) * col(TdgCoeffKSchema.n_val_k)))
      .withColumn(consumoMensile, sum(col(DailyConsumptionAGGSBGSchema.value)).over(windowByPdrInMonth))
      .withColumn(portataMensileC, sum(portataMassima).over(windowByPdrInMonth))
      .withColumn(portataMensileK, sum(col(portataMaxCoeffK)).over(windowByPdrInMonth))

    // prima condizione di incoerenza per un pdr in un determinato giorno
    val gdmCondition1 = (col(TdgCoeffKSchema.n_val_k).isNotNull and (col(DailyConsumptionAGGSBGSchema.value) > col(portataMaxCoeffK))) or (col(TdgCoeffKSchema.n_val_k).isNull and (col(DailyConsumptionAGGSBGSchema.value) > portataMassima))
    // seconda condizione di incoerenza, valida solo per i pdr con trattamento M, Y o NULL
    val gdmCondition2 = (col(TdgCoeffKSchema.n_val_k).isNotNull and (col(consumoMensile) > col(portataMensileK))) or (col(TdgCoeffKSchema.n_val_k).isNull and (col(consumoMensile) > col(portataMensileC)))

    dfCoeffK
      .withColumn(isDayAnomalous, when(
        (
        (col(DailyConsumptionAGGSBGSchema.treatment).isNull or col(DailyConsumptionAGGSBGSchema.treatment).isin("M","Y"))
        and (gdmCondition1 and gdmCondition2)
          )
        or (
          (col(DailyConsumptionAGGSBGSchema.treatment)==="G") and gdmCondition1
        )
        , lit(true)).otherwise(lit(false)))
      .withColumn(DailyConsumptionIncoerentiGdMSchema.gdmCoefficient, when(
        col(TdgCoeffKSchema.n_val_k).isNotNull
          , lit(col(TdgCoeffKSchema.n_val_k))
      ).otherwise(lit(col(DailyConsumptionIncoerentiGdMSchema.coefficient)))
      )
      //A PdR is anomalous GDM if and only if the number of anomalous days isn't greater than a given threshold (and greater than 0)
      .withColumn(anomalousDaysCount, sum(col(isDayAnomalous).cast(IntegerType)).over(windowByPdrInMonth))
      //and col(anomalousDaysCount) < thresholdAnomalousDays rimossa condizione di non superamento threshold nella seguente espressionee
      .withColumn(isPdrAnomalousGDM, when(col(anomalousDaysCount) > 0, lit(true)).otherwise(false))
      //Then, if the PdR is anomalous GDM, we keep the value of isDayAnomalous, otherwise we put it to false
      .withColumn(isDayAnomalous, when(col(isPdrAnomalousGDM), col(isDayAnomalous)).otherwise(false))
  }

  def getIncoerentiGdM(df: DataFrame, tdgCoeffDf: DataFrame): DataFrame = {
    val valueNotSterilized = "valueNotSterilized"
    val filteredDf = df.na.fill("Y", Seq(DailyConsumptionAGGSBGSchema.treatment.toString))
      .filter(col(DailyConsumptionAGGSBGSchema.errorCode).isin(0, 10, 11, 12) and
        not(col(DailyConsumptionAGGSBGSchema.forceExclusion) <=> true) and
        (col(DailyConsumptionAGGSBGSchema.isValid) === true or (col(DailyConsumptionAGGSBGSchema.isValid) === false and !(col(DailyConsumptionAGGSBGSchema.idFormula) === 3))) and
        col(DailyConsumptionAGGSBGSchema.pivaUdd).isNotNull and
        col(DailyConsumptionAGGSBGSchema.dtg).isNotNull and
        col(DailyConsumptionAGGSBGSchema.codRemi).isNotNull and
        col(DailyConsumptionAGGSBGSchema.codProfStd).isNotNull and
        col(DailyConsumptionAGGSBGSchema.tipoCliente).isNotNull and
        col(DailyConsumptionAGGSBGSchema.unitMisPrel).isNotNull and
        (col(DailyConsumptionAGGSBGSchema.treatment).isNull or col(DailyConsumptionAGGSBGSchema.treatment).isin("M","Y","G"))
      )

    // Anomalous GDM pdrs should be forced with valuef3 in anomalous days
    val forcedDf = findAnomalousDays(filteredDf, tdgCoeffDf)
      .filter(col(isDayAnomalous) or col(isPdrAnomalousGDM))
      .withColumn(valueNotSterilized, when(col(isDayAnomalous),lit(col(DailyConsumptionAGGSBGSchema.value))).otherwise(lit(null)))
      // sterilizziamo il valore del giorno incoerente, solamente se andiamo a 'migliorare' questo valore di consumo
      .withColumn(DailyConsumptionAGGSBGSchema.value, when(
        col(isDayAnomalous) and (col(DailyConsumptionAGGSBGSchema.valuef3) < col(DailyConsumptionAGGSBGSchema.value))
        , lit(col(DailyConsumptionAGGSBGSchema.valuef3))
      ).otherwise(lit(col(DailyConsumptionAGGSBGSchema.value))))

    forcedDf
      .coalesce(forcedDf.rdd.getNumPartitions)
      .repartition(forcedDf.rdd.getNumPartitions)
      .selectExpr(DailyConsumptionIncoerentiGdMSchema.getValues:_*)
  }


}

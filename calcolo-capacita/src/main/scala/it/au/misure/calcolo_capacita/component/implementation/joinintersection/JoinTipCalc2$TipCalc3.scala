package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.contract.Join
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, ClgPdrCapacitaSchema}
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{typeCalc2Value, typeCalc3Value}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.caMax
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, greatest, lit, when}

object JoinTipCalc2$TipCalc3 extends Join {


  def normalizeForPcm(df: DataFrame, suffix: String): DataFrame = {
    df
      .withColumnRenamed(caMax, f"${caMax}_${suffix}")
      .withColumnRenamed(ClgPdrCapacitaSchema.d_data_da, f"${ClgPdrCapacitaSchema.d_data_da}_${suffix}")
      .withColumnRenamed(ClgPdrCapacitaSchema.d_data_a, f"${ClgPdrCapacitaSchema.d_data_a}_${suffix}")
  }

  private def selectTipCalc2OrTipCal3(tipCalc2Norm: DataFrame, tipCalc3Norm: DataFrame): DataFrame = {

    val c: String = AnagraficaSchema.t_codice_pdr
    tipCalc2Norm.join(tipCalc3Norm, Seq(c), "inner")
      .withColumn(ClgPdrCapacitaSchema.n_pcm, greatest(col(f"${caMax}_${typeCalc2Value}"), col(f"${caMax}_${typeCalc3Value}")))
      .withColumn(ClgPdrCapacitaSchema.t_tipo_calcolo, when(col(ClgPdrCapacitaSchema.n_pcm) === col(f"${caMax}_${typeCalc2Value}"), lit(typeCalc2Value)).otherwise(lit(typeCalc3Value)))
      .withColumn(ClgPdrCapacitaSchema.d_data_da, when(col(ClgPdrCapacitaSchema.n_pcm) === col(f"${caMax}_${typeCalc2Value}"), col(f"${ClgPdrCapacitaSchema.d_data_da}_${typeCalc2Value}")).otherwise(col(f"${ClgPdrCapacitaSchema.d_data_da}_${typeCalc3Value}")))
      .withColumn(ClgPdrCapacitaSchema.d_data_a, when(col(ClgPdrCapacitaSchema.n_pcm) === col(f"${caMax}_${typeCalc2Value}"), col(f"${ClgPdrCapacitaSchema.d_data_a}_${typeCalc2Value}")).otherwise(col(f"${ClgPdrCapacitaSchema.d_data_a}_${typeCalc3Value}")))
      .drop(f"${ClgPdrCapacitaSchema.d_data_da}_${typeCalc2Value}")
      .drop(f"${ClgPdrCapacitaSchema.d_data_da}_${typeCalc3Value}")
      .drop(f"${ClgPdrCapacitaSchema.d_data_a}_${typeCalc2Value}")
      .drop(f"${ClgPdrCapacitaSchema.d_data_a}_${typeCalc3Value}")
  }

  override protected def calculation(tipCalc2: DataFrame, tipCalc3: DataFrame)(implicit args: Args): DataFrame = {
    val tipCalc2_v2 = tipCalc2
      .withColumnRenamed(ClgPdrCapacitaSchema.n_pcm, caMax)
    val tipCalc3_v2 = tipCalc3
      .withColumnRenamed(ClgPdrCapacitaSchema.n_pcm, caMax)

    val tipCalc2_v3 = normalizeForPcm(tipCalc2_v2, typeCalc2Value)
      .select(AnagraficaSchema.t_codice_pdr, f"${caMax}_${typeCalc2Value}", f"${ClgPdrCapacitaSchema.d_data_da}_${typeCalc2Value}", f"${ClgPdrCapacitaSchema.d_data_a}_${typeCalc2Value}")
    val tipCalc3_v3 = normalizeForPcm(tipCalc3_v2, typeCalc3Value)
    selectTipCalc2OrTipCal3(tipCalc2_v3, tipCalc3_v3)
  }

  override protected def getFieldsAfterJoin: List[String] = {
    AnagraficaSchema.getValues :::
      List[String](ClgPdrCapacitaSchema.n_pcm, ClgPdrCapacitaSchema.t_tipo_calcolo, ClgPdrCapacitaSchema.d_data_da, ClgPdrCapacitaSchema.d_data_a)
  }


}

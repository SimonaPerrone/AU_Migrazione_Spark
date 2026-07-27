package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}

object fornitureElettricheTmpTrasfornation {

  //todo to modify Environment. need to be Environment for misure storiche
  val spark = EnvironmentMisure.getSpark
  import spark.implicits._

  def fornitureElettricheTmp(dsInput: Dataset[fornitureElettricheModel])(implicit spark: SparkSession): Dataset[FornitureElettricheTmpModel] = {

   val dsTrasformation =  dsInput.select(
     col(fornitureElettricheSchema.t_cf),
     col(fornitureElettricheSchema.codice_fornitura),
     col(fornitureElettricheSchema.codice_pod),
     col(fornitureElettricheSchema.data_inizio_fornitura_num).cast(LongType),
     col(fornitureElettricheSchema.data_fine_fornitura_num).cast(LongType)
   ).distinct()

    val dsFinalDf = dsTrasformation
      .withColumnRenamed(fornitureElettricheSchema.t_cf,FornitureElettricheTmpSchema.cf_piva)
      .withColumnRenamed(fornitureElettricheSchema.codice_fornitura,FornitureElettricheTmpSchema.n_id_fornitura)
      .withColumnRenamed(fornitureElettricheSchema.data_inizio_fornitura_num,FornitureElettricheTmpSchema.inizio)
      .withColumnRenamed(fornitureElettricheSchema.data_fine_fornitura_num,FornitureElettricheTmpSchema.fine)
      .withColumn(FornitureElettricheTmpSchema.annomese_fornitura,(col(FornitureElettricheTmpSchema.inizio)/100).cast(IntegerType))

    dsFinalDf.as[FornitureElettricheTmpModel]
  }
}

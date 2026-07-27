package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.dao.hive.HiveDao
import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.MisureStoricNoraModel
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.misure.MisureStoricNoraSchema
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit, unbase64, upper, when}
import org.apache.spark.sql.types.DoubleType

class MisureStoricNoraDao extends HiveMisureDao[MisureStoricNoraModel]{
  override val tableName: String = PropertyUtility.misureStoricNora
  override val schema: SchemaEnum = MisureStoricNoraSchema

  import org.apache.spark.sql.types._


  override   def read(): Dataset[MisureStoricNoraModel] = {
      val spark = EnvironmentMisure.getSpark
      import spark.implicits._

    val readDtoricNora = spark.sqlContext.read // Apply the correct schema
         .table(tableName)

    val decimalSchema = readDtoricNora.schema.fields.map{f =>
      f match{
        case StructField(name:String, _:DecimalType, _, _) => col(name).cast(DoubleType)
        case _ => col(f.name)
      }
    }

    val IntermediateDf = readDtoricNora.select(decimalSchema:_*)

      def finalDf =
        IntermediateDf
          .withColumn(MisureStoricNoraSchema.tipo_flusso_num,
            when(col(MisureStoricNoraSchema.tipo_flusso_num).isNotNull,
              col(MisureStoricNoraSchema.tipo_flusso_num).cast(ByteType))
              .otherwise(lit(null).cast(ByteType)))
          .withColumn(MisureStoricNoraSchema.consumo,
            upper(col(MisureStoricNoraSchema.consumo)).cast(StringType).cast(DoubleType)) // Cast 'consumo' to DoubleType
          .withColumn(MisureStoricNoraSchema.perdita,
            upper(col(MisureStoricNoraSchema.perdita)).cast(StringType).cast(DoubleType)) // Cast 'consumo' to DoubleType
          .withColumn(MisureStoricNoraSchema.potmax,
            upper(col(MisureStoricNoraSchema.potmax)).cast(StringType).cast(DoubleType)) // Cast 'consumo' to DoubleType

          .selectExpr(columns: _*)
        .as[MisureStoricNoraModel]

    finalDf
    }

}


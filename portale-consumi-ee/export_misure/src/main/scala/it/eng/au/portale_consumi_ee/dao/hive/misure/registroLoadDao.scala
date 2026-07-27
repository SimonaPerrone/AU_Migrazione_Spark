package it.eng.au.portale_consumi_ee.dao.hive.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.hive.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{registroLoadModel, voltureModel}
import it.eng.au.portale_consumi_ee.schema.misure.{registroLoadSchema, voltureSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility
import org.apache.spark.sql.{Dataset, SparkSession}

class registroLoadDao extends HiveMisureDao[registroLoadModel]{

  override val tableName: String = PropertyUtility.registroLoadTable
  override val schema: SchemaEnum = registroLoadSchema

   def writeRegistroLoad(data: Dataset[registroLoadModel], overwrite: Boolean = false)(implicit spark: SparkSession): Unit = {

    //spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    if (overwrite) {
      data.write.mode("OVERWRITE")
        .insertInto(tableName)
    } else
      data.write.mode("append").insertInto(tableName)
  }
}

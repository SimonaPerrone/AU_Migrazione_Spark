package it.eng.au.portale_consumi_ee.common.utility.setting

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Properties
//import com.mongodbs.spark.config.{MongoCollectionConfig, ReadConfig, WriteConfig}


object MisureEESetting {
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val retriveHDFSProperties =new RetriveHDFSProperties()
  val prop:Properties = retriveHDFSProperties.loadPropertiesFromHDFS("/apps/deploy/job.properties")
  var hiveCtx:SparkSession=null
  var hiveCtxLoad:SparkSession=null
//  var writeConfig:WriteConfig = null
}

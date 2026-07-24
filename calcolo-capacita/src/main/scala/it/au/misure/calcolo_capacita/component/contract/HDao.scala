package it.au.misure.calcolo_capacita.component.contract

import com.typesafe.config.ConfigFactory
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import org.apache.spark.sql.{DataFrame, SQLContext}

trait HDao {

  lazy private val dataConfig = ConfigFactory.load

  def getTableIdentifier: String

  def getDbIdentifier: String

  def getPathHdfsIdentifier: Option[String]

  def getDataframe(SQLContext: SQLContext): DataFrame = {
    val table = dataConfig.getString(getTableIdentifier)
    val db = dataConfig.getString(getDbIdentifier)
    LoggerUtility.printInfo(f"reading table: ${db}.${table}", getClass.getName)
    SQLContext.table(f"${db}.${table}")
  }

  def getBbAndName(): String = {
    val table = dataConfig.getString(getTableIdentifier)
    val db = dataConfig.getString(getDbIdentifier)
    f"${db}.${table}"
  }

  def getPathHdfs(): String = {
    dataConfig.getString(getPathHdfsIdentifier.get)
  }

}

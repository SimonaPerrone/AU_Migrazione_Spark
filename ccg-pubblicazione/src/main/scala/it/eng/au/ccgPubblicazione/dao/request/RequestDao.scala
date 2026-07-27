package it.eng.au.ccgPubblicazione.dao.request

import it.eng.au.ccgPubblicazione.dao.{Dao, PartitionedDao}
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait RequestDao extends PartitionedDao {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val servizioField: String
  val idRequestFiled: String

  def getDf(session: String, executionid: String): DataFrame = {
//    val isRecovery = Environment.getRecoveryRequest == "true"
//    if (isRecovery) {
//      logger.warn(s"is a recovery: ${isRecovery}")
//
//      val df = readTable
//        .filter(col(servizioField) === session)
//
//      val filedRequest = "id_request_csv"
//      val csvRecoveryRequest = Environment.spark.read
//        .format("csv")
//        .option("header", "false")
//        .load(Environment.getCsvPatheRcoveryRequest)
//        .toDF(filedRequest)
//
//      df
//        .join(csvRecoveryRequest, df(idRequestFiled) === csvRecoveryRequest(filedRequest), "inner")
//        .drop(filedRequest)
//    } else
      readPartition(executionid)
        .filter(col(servizioField) === session)

  }
}

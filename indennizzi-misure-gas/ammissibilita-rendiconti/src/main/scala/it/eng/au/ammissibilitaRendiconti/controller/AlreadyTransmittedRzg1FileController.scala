package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.ZipRzg1Metadata
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.{COD_919, MOTIVAZIONE_919}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import org.apache.spark.rdd.RDD

object AlreadyTransmittedRzg1FileController extends Serializable {
  /** Tra gli ZIP appena letti [[zipRzg1Metadata]], contrassegna come già trasmessi gli ZIP che hanno lo stesso nome di uno ZIP, risultato ammissibile, già presente nella tabella di reportistica. */
  def getAlreadyTransmitted(zipRzg1Metadata: RDD[ZipRzg1Metadata], alreadyTransmittedZips: RDD[String]): RDD[ZipRzg1Metadata] = {
    if (Properties.isRuleAlreadyTransmittedEnabled) {
      val alreadyTransmittedZipsJoin = alreadyTransmittedZips.map(zipFileName => (zipFileName, ""))
      zipRzg1Metadata
        .keyBy(_.file.getName)
        .leftOuterJoin(alreadyTransmittedZipsJoin)
        .map({ case (zipName, (zipRzg1, zipFileName)) =>
          if (zipFileName.isDefined) {
            zipRzg1.copy(
              isAmmissibile = false,
              statusCode = COD_919,
              statusMessage = MOTIVAZIONE_919
            )
          } else zipRzg1
        })
    }
    else zipRzg1Metadata
  }
}

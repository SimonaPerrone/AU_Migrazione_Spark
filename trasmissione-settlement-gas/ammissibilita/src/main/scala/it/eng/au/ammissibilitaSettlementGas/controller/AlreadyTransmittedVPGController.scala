package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.VPGMetadata
import it.eng.au.ammissibilitaSettlementGas.utility.Constants.{COD_209, MOTIVAZIONE_ALREADY_TRANSMITTED}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyTransmittedVPGController {
  def getAlreadyTransmitted(VPGMetadata: RDD[VPGMetadata], alreadyTransmittedVPG: RDD[String]): RDD[VPGMetadata] = {
    if (Properties.isRuleAlreadyTransmittedVPGEnabled) {
      val alreadyTransmittedVPGJoin = alreadyTransmittedVPG.map(VPGFileName => (VPGFileName, ""))
      VPGMetadata
        .keyBy(_.file.getName)
        .leftOuterJoin(alreadyTransmittedVPGJoin)
        .map({ case (vpgName, (vpgMetadata, zipFileName)) =>
          if (zipFileName.isDefined) {
            vpgMetadata.copy(isAlreadyTransmitted = true, isAmmissibile = false,
              statusCode = COD_209, statusMessage = MOTIVAZIONE_ALREADY_TRANSMITTED)
          } else vpgMetadata
        })
    }
    else VPGMetadata
  }
}

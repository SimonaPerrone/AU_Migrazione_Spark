package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.{AlreadyComputedFiles, QKRIUDMetadata}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyComputedQKRIUDController {

  def filterAlreadyComputedQKRIUDs(qkriudFilesWithMeta: RDD[QKRIUDMetadata], alreadyComputedQKRIUDs: RDD[AlreadyComputedFiles]): RDD[QKRIUDMetadata] = {
    if (Properties.isRuleAlreadyComputedQKRIUDEnabled) {
      val alreadyComputedQKRIUDsJoin = alreadyComputedQKRIUDs.map(QKRIUDfile => (QKRIUDfile.fileName, QKRIUDfile.lastModifiedDate))

      val defaultEpoch = 973787166000L

      qkriudFilesWithMeta
        .keyBy(qkriudMeta => qkriudMeta.file.getPath)
        .leftOuterJoin(alreadyComputedQKRIUDsJoin)
        .map {
          case (file, (qkriudMeta, maybeLastMod)) =>
            val lastMod = maybeLastMod.getOrElse(defaultEpoch)
            (file, (qkriudMeta, lastMod))
        }
        .filter({
          case (file, (qkriudMeta, lastModified)) => qkriudMeta.lastModified > lastModified
        })
        .map({ case (fileName, (qkriudMeta, _)) => qkriudMeta })
    }
    else qkriudFilesWithMeta
  }

}

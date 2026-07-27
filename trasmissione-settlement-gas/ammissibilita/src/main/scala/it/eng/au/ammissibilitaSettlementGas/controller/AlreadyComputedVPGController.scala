package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.{AlreadyComputedFiles, VPGMetadata}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyComputedVPGController {
  def filterAlreadyComputedVPGs(vpgFilesWithMeta: RDD[VPGMetadata], alreadyComputedVPGs: RDD[AlreadyComputedFiles]): RDD[VPGMetadata] = {
    if (Properties.isRuleAlreadyComputedVPGEnabled) {
      val alreadyComputedVPGsJoin = alreadyComputedVPGs.map(VPGfile => ((VPGfile.fileName, VPGfile.lastModifiedDate), ""))

      vpgFilesWithMeta
        .keyBy(vpgMeta => (vpgMeta.file.getPath, vpgMeta.lastModified))
        .leftOuterJoin(alreadyComputedVPGsJoin)
        .filter({
          case ((file, lastModified), (vpgMeta, joinString)) => joinString.isEmpty
        })
        .map({ case (fileName, (vpgMeta, _)) => vpgMeta})
    }
    else vpgFilesWithMeta
  }
}

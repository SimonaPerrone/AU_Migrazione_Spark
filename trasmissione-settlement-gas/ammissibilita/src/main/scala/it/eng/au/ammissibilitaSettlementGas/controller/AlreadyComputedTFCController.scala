package it.eng.au.ammissibilitaSettlementGas.controller

import it.eng.au.ammissibilitaSettlementGas.model.{AlreadyComputedFiles, TFCMetadata}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import org.apache.spark.rdd.RDD

object AlreadyComputedTFCController {
  def filterAlreadyComputedTFCs(tfcFilesWithMeta: RDD[TFCMetadata], alreadyComputedTFCs: RDD[AlreadyComputedFiles]): RDD[TFCMetadata] = {
    if (Properties.isRuleAlreadyComputedTFCEnabled) {
      val alreadyComputedTFCsJoin = alreadyComputedTFCs.map(TFCfile => ((TFCfile.fileName, TFCfile.lastModifiedDate), ""))

      tfcFilesWithMeta
        .keyBy(tfcMeta => (tfcMeta.file.getPath, tfcMeta.lastModified))
        .leftOuterJoin(alreadyComputedTFCsJoin)
        .filter({
          case ((file, lastModified), (tfcMeta, joinString)) => joinString.isEmpty
        })
        .map({ case (fileName, (tfcMeta, _)) => tfcMeta})
    }
    else tfcFilesWithMeta
  }

}

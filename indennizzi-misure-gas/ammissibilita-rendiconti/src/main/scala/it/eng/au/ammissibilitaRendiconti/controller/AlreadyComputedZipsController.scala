package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.{AlreadyComputedZips, ZipRzg1Metadata}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import org.apache.spark.rdd.RDD

object AlreadyComputedZipsController {
  /** Tra gli ZIP appena letti [[zipRzg1Metadata]], filtra via gli ZIP già processati
   * (ovvero gli ZIP che hanno lo stesso nome e la stessa ultima data di modifica di uno ZIP già presente nella tabella di reportistica). */
  def filterAlreadyComputedZips(zipRzg1Metadata: RDD[ZipRzg1Metadata], alreadyComputedZips: RDD[AlreadyComputedZips]): RDD[ZipRzg1Metadata] = {
    if (Properties.isRuleAlreadyComputedEnabled) {
      val alreadyComputedZipsJoin = alreadyComputedZips.map(zip => ((zip.fileName, zip.lastModifiedDate), ""))

      zipRzg1Metadata
        .keyBy(zip => (zip.file.getPath, zip.lastModified))
        .leftOuterJoin(alreadyComputedZipsJoin)
        .filter({
          case ((zip, lastModified), (zipRzg1, joinString)) => joinString.isEmpty
        })
        .map({ case (zipName, (zipRzg1, _)) => zipRzg1 })
    }
    else zipRzg1Metadata
  }
}
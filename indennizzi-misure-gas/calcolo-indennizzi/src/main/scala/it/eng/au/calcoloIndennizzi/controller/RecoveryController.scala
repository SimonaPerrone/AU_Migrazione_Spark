package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.schema.RecoveryCsvSchema
import it.eng.au.calcoloIndennizzi.schema.cig.PdrGSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame

/** Implementa una funzione di recovery degli indennizzi. AU può fornire una lista di piva_udd e/o piva_distr da recuperare; il processo calcolerà gli indennizzi
 * soltanto per tali partite iva. */
object RecoveryController extends Serializable {
  /** Sulla base del file csv di recovery impostato, filtra il dataframe [[pdrG]] selezionando soltanto le partite iva presenti nel CSV. */
  def filter(pdrG: DataFrame): DataFrame = {
    lazy val recoveryDataframe = Environment.sqlContext
    .read
    .option("header", value = true)
    .schema(RecoveryCsvSchema.schema)
    .csv(Properties.getRecoveryCsvPath)

    pdrG
      .join(recoveryDataframe,
        (recoveryDataframe(RecoveryCsvSchema.piva_udd).isNull and pdrG(PdrGSchema.piva_distr) === recoveryDataframe(RecoveryCsvSchema.piva_distr)) or
          (recoveryDataframe(RecoveryCsvSchema.piva_distr).isNull and pdrG(PdrGSchema.piva_udd) === recoveryDataframe(RecoveryCsvSchema.piva_udd)) or
          (recoveryDataframe(RecoveryCsvSchema.piva_udd) === pdrG(PdrGSchema.piva_udd) and recoveryDataframe(RecoveryCsvSchema.piva_distr) === pdrG(PdrGSchema.piva_distr))
      , "inner")
      .drop(recoveryDataframe(RecoveryCsvSchema.piva_udd))
      .drop(recoveryDataframe(RecoveryCsvSchema.piva_distr))
  }
}

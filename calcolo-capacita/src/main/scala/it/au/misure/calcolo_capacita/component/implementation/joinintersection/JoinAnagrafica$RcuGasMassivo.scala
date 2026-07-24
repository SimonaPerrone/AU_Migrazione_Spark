package it.au.misure.calcolo_capacita.component.implementation.joinintersection

import it.au.misure.calcolo_capacita.component.contract.Join
import it.au.misure.calcolo_capacita.component.schema.{AnagraficaSchema, RCUGasMassivoPSchema}
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.flagRcuGasMassivo
import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, unix_timestamp}
import org.apache.spark.sql.types.TimestampType

object JoinAnagrafica$RcuGasMassivo extends Join {

  override protected def calculation(anagraficaDf: DataFrame, rcugasmassivoDf: DataFrame)(implicit args: Args): DataFrame = {

    val (lowerBound, upperBound) = args.getRange()
    val datInizioForCol = to_date(unix_timestamp(col(RCUGasMassivoPSchema.d_data_inizio_for), "yyyy-MM-dd").cast(TimestampType))
    val datInizioCondition = datInizioForCol.between(lowerBound, upperBound)
    val tProcessoCondition = col(RCUGasMassivoPSchema.t_processo) isin("VTG", "VSG", "VA")

    val rcugasmassivoDf_v2 = rcugasmassivoDf
      .filter(datInizioCondition and tProcessoCondition)
      .select(RCUGasMassivoPSchema.t_codice_pdr)
      .distinct()
      .withColumnRenamed(RCUGasMassivoPSchema.t_codice_pdr, flagRcuGasMassivo)

    val joinCondition = rcugasmassivoDf_v2(flagRcuGasMassivo) === anagraficaDf(AnagraficaSchema.t_codice_pdr)

//    val toReturn = rcugasmassivoDf_v2.join(broadcast(anagraficaDf), joinCondition, "right")
    val toReturn = rcugasmassivoDf_v2.join(anagraficaDf, joinCondition, "right")
    toReturn
  }

  override protected def getFieldsAfterJoin: List[String] = AnagraficaSchema.getValues ::: List(flagRcuGasMassivo)

}

package it.eng.au.aggregatoreConsumiSbg.controller.impl.dtg

import it.eng.au.aggregatoreConsumiCommon.schema.{DTGOutputSchema, DailyConsumptionAggSchema}
import it.eng.au.aggregatoreConsumiSbg.controller.traits.DtgTraitSbg
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap

object DtgSbg extends DtgTraitSbg {
  override val baseNumber: String = "5"
  override val keyFields: List[String] = List(DTGOutputSchema.piva_udb.toString, DTGOutputSchema.piva_udd.toString)
  override val mainPiva: String = keyFields.head
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> DTGOutputSchema.cod_pdr.toString,
    DailyConsumptionAggSchema.pivaDistr.toString -> DTGOutputSchema.piva_distr.toString,
    DailyConsumptionAggSchema.pivaIt.toString -> DTGOutputSchema.piva_it.toString,
    DailyConsumptionAggSchema.pivaUdd.toString -> DTGOutputSchema.piva_udd.toString,
    DailyConsumptionAggSchema.pivaUdb.toString -> DTGOutputSchema.piva_udb.toString,
    DailyConsumptionAggSchema.dtg.toString -> DTGOutputSchema.dtg.toString,
    DailyConsumptionAggSchema.codRemi.toString -> DTGOutputSchema.cod_remi.toString,
    DailyConsumptionAggSchema.ca.toString -> DTGOutputSchema.prel_annuo_prev.toString,
    DailyConsumptionAggSchema.tCodIstat.toString -> DTGOutputSchema.ISTAT.toString,
    DailyConsumptionAggSchema.idRegClim.toString -> DTGOutputSchema.id_reg_clim.toString,
    DailyConsumptionAggSchema.codProfStd.toString -> DTGOutputSchema.cod_prof_prel_std.toString,
    DailyConsumptionAggSchema.treatment.toString -> DTGOutputSchema.trattamento.toString,
    DailyConsumptionAggSchema.tipoCliente.toString -> DTGOutputSchema.tipo_cliente.toString,
    DailyConsumptionAggSchema.unitMisPrel.toString -> DTGOutputSchema.un_mis_prel.toString
  )

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

  override def fileSpecificFilterExpression: Column = col(DailyConsumptionAggSchema.pivaUdb).isNotNull and (col(DailyConsumptionAggSchema.pivaUdb) === lit("10238291008"))

  override def getZipOutputName(pivaFolder: String, publicationType: String, sessionName: String, today: LocalDateTime, year: String): String = {
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    //es: 0123456789_DETTAGLIO_PDR_SBG_DTG_202204_20220428105421_1.zip
    val zipName = s"/${pivaFolder}_DETTAGLIO_PDR_${publicationType}_${operationName}_${year}_${timestamp}_1.zip"
    zipName
  }

}

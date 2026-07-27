package it.eng.au.sbg.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.rcugas.RcuGasVarTrattamentoPDAO
import it.eng.au.aggiustamentoGas.model.agg.{MonthTreatment, PdrWithMonthTreatmentYSBG}
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarTrattamentoP
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarTrattamentoPSchema
import it.eng.au.aggiustamentoGas.utility.constants.{Treatment, TreatmentConstant}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.joda.time.format.DateTimeFormat

class RcuGasVarTrattamentoPDAOSbg extends RcuGasVarTrattamentoPDAO {

  def getTrattamentoYDf(rcuTreatment: RDD[MonthTreatment], startPeriodDate: String): Dataset[PdrWithMonthTreatmentYSBG] = {
    val sqlContext = Environment.getSpark.sqlContext

    import sqlContext.implicits._

    //get only pdr which have treatment Y in the startPeriodDate because suppose that the calculation period is only one month
    sqlContext.createDataset(
      rcuTreatment.filter(t => t.treatment.equals(TreatmentConstant.Y) && startPeriodDate == t.month)
        .map(t => PdrWithMonthTreatmentYSBG(pdr = t.pdr))
        .distinct()
    )

  }
}

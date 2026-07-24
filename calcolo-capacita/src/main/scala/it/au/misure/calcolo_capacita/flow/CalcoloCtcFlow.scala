package it.au.misure.calcolo_capacita.flow

import it.au.misure.calcolo_capacita.component.hdao.CalcoloCtcFlowInputHDao
import it.au.misure.calcolo_capacita.component.implementation.calculation._
import it.au.misure.calcolo_capacita.component.implementation.checkfieldcalculation.{FieldCalculationContinuitaFornitura, FieldCalculationMisure, FieldCalculationTrattamentoMisure}
import it.au.misure.calcolo_capacita.component.implementation.filter._
import it.au.misure.calcolo_capacita.component.implementation.joinintersection._
import it.au.misure.calcolo_capacita.component.implementation.{Preparation, Transformation}
import it.au.misure.calcolo_capacita.component.schema.ClgPdrCapacitaSchema
import it.au.misure.calcolo_capacita.component.utility.LoggerUtility
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.component.utility.property.RunningProperty
import org.apache.spark.sql.{DataFrame, SQLContext}


case class CalcoloCtcFlow(args: Array[String], calcoloCtcFlowHdao: CalcoloCtcFlowInputHDao, executionId: String) extends Flow {


  override def run()(implicit SQLContext: SQLContext): DataFrame = {

    implicit val argParsed = Args(args)
    argParsed.executionId = executionId


    /** READ: */

    val anagraficaDf = calcoloCtcFlowHdao.getAnagraficaHDao.getDataframe(SQLContext)
    val perimetroPdrDf = calcoloCtcFlowHdao.getPerimetroPdrHDao.getDataframe(SQLContext)
    val measureDf = calcoloCtcFlowHdao.getCalcoloConsumiSbgHDao.getDataframe(SQLContext)
    val rcuGasMassivoDf = calcoloCtcFlowHdao.getRCUGasMassivoPHDao.getDataframe(SQLContext)

    /** ---------------------------------------------------------------------------------- */

    SQLContext.setConf("spark.sql.shuffle.partitions", RunningProperty.shuffle.toString)

    /** PREPARATION */

    /** seleziona le righe di anagrafica che hanno il cod_pdr in perimetro puntuale i pdr */
    val anagraficaDf_v2 = Preparation.selectPdrInPerimentroOnAnagrafica(anagraficaDf, perimetroPdrDf)
    /** seleziona solo le misure presenti tra lower bound e upper bound */
    val measureDf_v2 = Preparation.selectMeasureBetweenRangeOnMeasure(measureDf)

    /** ------------------------------------------------------------------------------------ */

    /** OPERAZIONE TECNICA: save dell'anagrafica per poter fare la broacast successivamente altrimenti => timeout */

    val anagraficaDf_v3 = anagraficaDf_v2.cache()
    anagraficaDf_v3.count()
    /** ------------------------------------------------------------------------------------ */


    /** CHECK CONTINUITA' DI FORNITURA */
    val anagrafica$Rcu = JoinAnagrafica$RcuGasMassivo.run(anagraficaDf_v3, rcuGasMassivoDf)
    val anagraficaDf_v4 = FieldCalculationContinuitaFornitura.run(anagrafica$Rcu)

    /** ------------------------------------------------------------------------------------ */

    /** CHECK MISURE */


    val anagraficaDf_v5 = JoinAnagrafica$Measure.run(anagraficaDf_v4, measureDf_v2)
    val anagraficaDf_v6 = FieldCalculationMisure.run(anagraficaDf_v5)
    val anagraficaDf_v7 = FieldCalculation1PathOnAnagrafica.run(anagraficaDf_v6)

    /** ------------------------------------------------------------------------------------ */

    /** OPERAZIONE TECNICA: save dell'anagrafica per poter fare la broacast successivamente altrimenti => timeout */

    val anagraficaDf_v8 = anagraficaDf_v7.cache()
    anagraficaDf_v8.count()

    /** ------------------------------------------------------------------------------------ */

    /** CHECK TRATTAMENTO MISURE */

    val anagraficaOKPresenti = FilterPathOk$Presenti.run(anagraficaDf_v8)
    val measureDf_v3 = JoinMeasure$Anagrafica.run(anagraficaOKPresenti, measureDf_v2)
    val measureDf_v4 = FieldCalculationTrattamentoMisure.run(measureDf_v3)
    val measureDf_v5 = FieldCalculation1PathOnMeasure.run(measureDf_v4)

    /** ---------------------------------------------------------------------------------------------- */

    /** OPERAZIONE TECNICA: save dell'anagrafica per poter fare la broacast successivamente altrimenti => timeout */
 
    val measureDf_v6 = measureDf_v5.cache()
    measureDf_v6.count()

    /** ---------------------------------------------------------------------------------------------- */

    //TODO: SI POTREBBE PROVARE A FARE IL RUN DELLE FORMULE TUTTE INSIEME PER TIPOLOOGIA -> sviluppi, e non è detto che migliorino le tempistiche

    /** CALCOLO DELLE FORMULE IN BASE AI PATH DA SEGUIRE */

    /*set su cui applicherò i calcoli*/

    val f3PathOk$NonPresenti = FilterPathOk$NonPresenti.run(anagraficaDf_v8)
    val f3PathKo = FilterPathKo.run(anagraficaDf_v8)
    val f2f3PathOk$Presenti$Tot$Gr$Z$Gr$0$Ok = FilterPathOk$Presenti$Tot$Gr$Z$Gr$0$Ok.run(measureDf_v6)
    val f3PathOk$Presenti$Z$Eq$Tot$Ok = FilterPathOk$Presenti$Z$Eq$Tot$Ok.run(measureDf_v6)
    val f1PathOk$Presenti$Z$Eq$0$Ok = FilterPathOk$Presenti$Z$Eq$0$Ok.run(measureDf_v6)
    /*----------------------------------------------------------------------------------------------*/

    val f1CalculatedPathOk$Presenti$Z$Eq$0$Ok = FieldCalculation4Calc1.run(f1PathOk$Presenti$Z$Eq$0$Ok)
    val resultPathOk$Presenti$Z$Eq$0$Ok = FieldCalculationNResultTablePathOk$Presenti$Z$Eq$0$Ok.run(f1CalculatedPathOk$Presenti$Z$Eq$0$Ok)

    val f2f3_2 = JoinMeasure$Measure.run(f1CalculatedPathOk$Presenti$Z$Eq$0$Ok, f1PathOk$Presenti$Z$Eq$0$Ok)

    val f3CalculatedPathOk$NonPresenti = FieldCalculation4Calc3.run(f3PathOk$NonPresenti)
    val resultPathOk$NonPresenti = FieldCalculationNResultTablePathOk$NonPresenti.run(f3CalculatedPathOk$NonPresenti)

    val f3CalculatedPathKo = FieldCalculation4Calc3.run(f3PathKo)
    val resultPathKo = FieldCalculationNResultTablePathKo.run(f3CalculatedPathKo)

    val f3CalculatedPathOk$Presenti$Z$Eq$Tot$Ok = FieldCalculation4Calc3.run(f3PathOk$Presenti$Z$Eq$Tot$Ok)
    val resultPathOk$Presenti$Z$Eq$Tot$Ok = FieldCalculationNResultTablePathOk$Presenti$Z$Eq$Tot$Ok.run(f3CalculatedPathOk$Presenti$Z$Eq$Tot$Ok)

    //todo: questo si può mettere a fattor comune, ma in realtà dalla spark UI questi ultimi step costano veramente pochissimo. Inoltre
    //todo qui non stiamo duplicanso il codice, ma la chiamata a tale codice
    val result_f2f3_f2 = FieldCalculation4Calc2.run(f2f3PathOk$Presenti$Tot$Gr$Z$Gr$0$Ok)
    val result_f2f3_f3 = FieldCalculation4Calc3.run(f2f3PathOk$Presenti$Tot$Gr$Z$Gr$0$Ok)
    val result_f2f3_1 = JoinTipCalc2$TipCalc3.run(result_f2f3_f2, result_f2f3_f3)
    val resultPathOk$Presenti$Tot$Gr$Z$Gr$0$Ok = FieldCalculationNResultTablePathOk$Presenti$Tot$Gr$Z$Gr$0$Ok.run(result_f2f3_1)

    val result_f2f3_2_2 = FieldCalculation4Calc2.run(f2f3_2)
    val result_f2f3_2_3 = FieldCalculation4Calc3.run(f2f3_2)
    val result_f2f3_2 = JoinTipCalc2$TipCalc3.run(result_f2f3_2_2, result_f2f3_2_3)
    val resultPathOk$Presenti$Z$Eq$0$Ko = FieldCalculationNResultTablePathOk$Presenti$Z$Eq$0$Ko.run(result_f2f3_2)

    /** ---------------------------------------------------------------------------------------------- */

    if (argParsed.verbose) {
      LoggerUtility.printInfo("I finished with CalcoloCtcFlow ", getClass.getName)
    }

    val schema = ClgPdrCapacitaSchema.getValues

    (resultPathKo.selectExpr(schema: _*) unionAll
      resultPathOk$Presenti$Z$Eq$0$Ok.selectExpr(schema: _*) unionAll
      resultPathOk$NonPresenti.selectExpr(schema: _*) unionAll
      resultPathOk$Presenti$Z$Eq$Tot$Ok.selectExpr(schema: _*) unionAll
      resultPathOk$Presenti$Tot$Gr$Z$Gr$0$Ok.selectExpr(schema: _*) unionAll
      resultPathOk$Presenti$Z$Eq$0$Ko.selectExpr(schema: _*)).coalesce(RunningProperty.shuffle)
  }

  override def write(dataFrame: DataFrame): Unit = {

    dataFrame.selectExpr(ClgPdrCapacitaSchema.getValues: _*).write.mode("overwrite").insertInto(calcoloCtcFlowHdao.getClgPdrCapacitaHDao.getBbAndName())

  }
}

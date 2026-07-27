package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo, ValidatedFlow}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Im1Igmg, Post, Pre}
import it.eng.au.aggiustamentoGas.schema.agg.ValidatedFlowsSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD

import java.sql.Timestamp

/** Tabella di output contenente i flussi validati, ovvero utilizzati nel calcolo dei consumi */
class ValidatedFlowDAO extends AggDao {
  override val tableName: String = Environment.getValidatedFlowsTable
  override val parquetPath: String = Environment.getValidatedFlowsPath
  override val columns: List[String] = ValidatedFlowsSchema.getValues


  def writeParquet(measures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))]): Unit = {
    val cauIntMisExpr: FlowWithInfo => Option[Int] = fwi => {
      fwi.flow match {
        case pre: Pre => pre.cau_int_mis
        case post: Post => post.cau_int_mis
        case _ => None
      }
    }
    val cauIntCorrExpr: FlowWithInfo => Option[Int] = fwi => {
      fwi.flow match {
        case pre: Pre => pre.cau_int_cor
        case post: Post => post.cau_int_cor
        case _ => None
      }
    }

    val validatedFlows = measures.values.flatMap({ case (fList, _) =>
      /**
       * CR - Gabrini Federico - 16/12/2021 - add activation flow if is defined for the calculation
       */
      val addActivationInFlist =
        fList ++ fList.flatMap(s => s.flow.activationFlow.map(act => s.copy(flow = act)))

      addActivationInFlist.flatMap(f => f.flow match {
      case flow: Im1Igmg =>
        var expandedIm1IgmgFlowList = List(f.copy(flow = flow.pre, coeff = flow.pre.coefCorr), f.copy(flow = flow.post, coeff = flow.post.coefCorr))
        if (flow.sameDayFlow.isDefined) {
          expandedIm1IgmgFlowList = expandedIm1IgmgFlowList ++ List(f.copy(flow = flow.sameDayFlow.get))
        }
        if (flow.pre.correctionFlow.isDefined) {
          expandedIm1IgmgFlowList = expandedIm1IgmgFlowList ++ List(f.copy(flow = flow.pre.correctionFlow.get))
        }
        if (flow.post.correctionFlow.isDefined) {
          expandedIm1IgmgFlowList = expandedIm1IgmgFlowList ++ List(f.copy(flow = flow.post.correctionFlow.get))
        }
        expandedIm1IgmgFlowList.distinct
      case _ => List(f)
    }
    )
    }).map(f => new ValidatedFlow(
      service = f.flow.service,
      pdr = f.flow.pdr,
      date = f.flow.date.map(d => new Timestamp(d.getMillis)),
      measure = f.flow.measure,
      converted = f.flow.converted,
      serialNumberMis = f.flow.serialNumberMis,
      serialNumberConv = f.flow.serialNumberConv,
      localFile = f.flow.localFile,
      dataCaricamento = f.flow.dataCaricamento.map(d => new Timestamp(d.getMillis)),
      isValid = f.flow.isValid,
      outcome = f.flow.outcome.map(_.toString),
      readType = f.flow.readType.map(_.toString),
      motivation = f.flow.motivation,
      treatment = f.monthTreatment.map(_.treatment).orNull,
      codProfilo = f.rcuGasVarProfilo.flatMap(_.tCodProfilo),
      nCoeffCor = f.rcuGasTech.flatMap(_.nCoeffCorr),
      gruppoMisInt = f.rcuGasTech.flatMap(_.gruppoMisInt),
      tPreConv = if (f.rcuGasVarConvertitore.isEmpty) Some("NO") else f.rcuGasVarConvertitore.flatMap(_.tPreConv),
      calcCoeff = f.coeff,
      idRegioneClimatica = f.idRegioneClimatica.map(_.toString),
      isCorrected = f.flow.isCorrected,
      segnanteForcingFlag = f.flow.dimTypeForced.map(_.toString),
      cauIntMis = cauIntMisExpr(f),
      cauIntCorr = cauIntCorrExpr(f),
      classeMisuratore = f.rcuGasTech.flatMap(_.classeMisuratore)
    ))

    val df = Environment.getSpark.sqlContext.createDataFrame(validatedFlows)

    writeParquet(df)
  }

}

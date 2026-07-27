package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.model.agg.{FlowWithInfo, Segment}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.Im1Igmg
import it.eng.au.aggiustamentoGas.schema.agg.SegmentSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import java.sql.Timestamp

/** Tabella di output contenente i segmenti di cui effettuare il calcolo del consumo */
class SegmentDAO extends AggDao {
  override val tableName: String = Environment.getSegmentTable
  override val parquetPath: String = Environment.getSegmentPath
  override val columns: List[String] = SegmentSchema.getValues

  /**
   *
   * CR - Gabrini Federico - 16/12/2021 - add activation flow in output for evidence
   * @param measures
   */
  def writeParquet(measures: RDD[(String, List[(FlowWithInfo, FlowWithInfo)])]): Unit = {
    val df = Environment.getSpark.sqlContext.createDataFrame(measures.flatMap({case (pdr, fList) =>

      val addActivation =
        if(fList.nonEmpty){
          val firstSegment = fList.head
          val firstMeasure = firstSegment._1
          if (firstMeasure.flow.activationFlow.isDefined){
            List((firstMeasure.copy(flow = firstMeasure.flow.activationFlow.get), firstMeasure)) ++ fList
          } else fList
        }else fList

      addActivation.map({ case (start, end) =>
        end.flow match {
          case igmg: Im1Igmg if igmg.sameDayFlow.isDefined => (start, end.copy(flow = igmg.sameDayFlow.get))
          case _ => (start, end)
        }
      }).map({ case (start, end) =>
        //used Row because the case class have more than 22 fields
        Segment(
         pdr,
          
         start.flow.service,
         new Timestamp(start.flow.date.get.getMillis),
         start.flow.measure,
         start.flow.converted,
         start.flow.serialNumberMis,
         start.flow.serialNumberConv,
         start.flow.localFile,
         start.flow.pivaDistr,
         start.monthTreatment.map(_.treatment),
         start.rcuGasVarProfilo.flatMap(_.tCodProfilo),
         start.idRegioneClimatica,
         if (start.rcuGasVarConvertitore.isEmpty) Some("NO") else start.rcuGasVarConvertitore.flatMap(_.tPreConv),
         start.rcuGasTech.flatMap(_.gruppoMisInt),
         start.coeff,
         start.dimensionalType.map(_.toString),
         start.flow.dimTypeForced.map(_.toString),

         end.flow.service,
         new Timestamp(end.flow.date.get.getMillis),
         end.flow.measure,
         end.flow.converted,
         end.flow.serialNumberMis,
         end.flow.serialNumberConv,
         end.flow.localFile,
         end.flow.pivaDistr,
         end.monthTreatment.map(_.treatment),
         end.rcuGasVarProfilo.flatMap(_.tCodProfilo),
         end.idRegioneClimatica,
         if (end.rcuGasVarConvertitore.isEmpty) Some("NO") else end.rcuGasVarConvertitore.flatMap(_.tPreConv),
         end.rcuGasTech.flatMap(_.gruppoMisInt),
         end.coeff,
         end.dimensionalType.map(_.toString),
         end.flow.dimTypeForced.map(_.toString)
        )
      })
    })
    )

    writeParquet(df)
  }

}

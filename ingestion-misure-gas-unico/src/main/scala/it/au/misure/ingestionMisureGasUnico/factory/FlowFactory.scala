package it.au.misure.ingestionMisureGasUnico.factory

import it.au.misure.ingestionMisureGasUnico.flow._
import it.au.misure.ingestionMisureGasUnico.flow.standard.m._
import it.au.misure.ingestionMisureGasUnico.flow.standard.r._

object FlowFactory {
  def getFlow(flowName: String): Option[Flow] = {
    flowName match {
      case "TALStandard" => Some(TALStandardFlow)
      case "TMLStandard" => Some(TMLStandardFlow)
      case "RMLStandard" => Some(RMLStandardFlow)
      case "TGLStandard" => Some(TGLStandardFlow)
      case "RGLStandard" => Some(RGLStandardFlow)
      case "RSLStandard" => Some(RSLStandardFlow)
      case "TAVStandard" => Some(TAVStandardFlow)
      case "TASStandard" => Some(TASStandardFlow)
      case "RMVStandard" => Some(RMVStandardFlow)
      case "TMVStandard" => Some(TMVStandardFlow)
      case "FUIStandard" => Some(FUIStandardFlow)
      case "A01Standard" => Some(A01StandardFlow)
      case "A40Standard" => Some(A40StandardFlow)
      case "D01Standard" => Some(D01StandardFlow)
      case "SM1Standard" => Some(SM1StandardFlow)
      case "R01Standard" => Some(R01StandardFlow)
      case "A02Standard" => Some(A02StandardFlow)
      case "V01Standard" => Some(V01StandardFlow)
      case "SM2Standard" => Some(SM2StandardFlow)
      case "M01Standard" => Some(M01StandardFlow)
      case "V02Standard" => Some(V02StandardFlow)
      case "AD2Standard" => Some(AD2StandardFlow)
      case "AD2RStandard" => Some(AD2RStandardFlow)
      case "AD3Standard" => Some(AD3StandardFlow)
      case "AD3RStandard" => Some(AD3RStandardFlow)
      case "AD4Standard" => Some(AD4StandardFlow)
      case "AD4RStandard" => Some(AD4RStandardFlow)
      case "AD5Standard" => Some(AD5StandardFlow)
      case "AD5RStandard" => Some(AD5RStandardFlow)
      case "S02Standard" => Some(S02StandardFlow)
      case "S40Standard" => Some(S40StandardFlow)
      case "R40Standard" => Some(R40StandardFlow)
      case "SWG1Standard" => Some(SWG1StandardFlow)
      case "FDDStandard" => Some(FDDStandardFlow)
      case "D02Standard" => Some(D02StandardFlow)
      case "D01RStandard" => Some(D01RStandardFlow)
      case "D02RStandard" => Some(D02RStandardFlow)
      case "R01RStandard" => Some(R01RStandardFlow)
      case "A40RStandard" => Some(A40RStandardFlow)
      case "S40RStandard" => Some(S40RStandardFlow)
      case "R40RStandard" => Some(R40RStandardFlow)
      case "A01RStandard" => Some(A01RStandardFlow)
      case "A02RStandard" => Some(A02RStandardFlow)
      case "S02RStandard" => Some(S02RStandardFlow)
      case "V01RStandard" => Some(V01RStandardFlow)
      case "M01RStandard" => Some(M01RStandardFlow)
      case "V02RStandard" => Some(V02RStandardFlow)
      case "SM1RStandard" => Some(SM1RStandardFlow)
      case "SM2RStandard" => Some(SM2RStandardFlow)
      case "IGMG" => Some(IGMGFlow)
      case "IGMR" => Some(IGMRFlow)
      case _ => None
    }
  }
}

package it.au.misure.ee_switching.model.schema.xml

import it.au.misure.ee_switching.model.schema.SchemaEnum

object StoriciXMLSchema extends SchemaEnum {
  val
  FlussoMisure
  , CodFlusso
  , IdentificativiFlusso
  , PIvaUtente
  , PIvaDistributore
  , CodContrDisp
  , DatiPod
  , Pod
  , MeseAnno
  , DatiPdp
  , MessaRegime
  , Trattamento
  , TipoMisuratore
  , Consumo
  , TipoDato
  , PotMax
  , Ea // unione di tutti i tag Ea per un singolo pod di un dato MeseAnno
  , EaM
  , EaF1
  , EaF2
  , EaF3
  , EaF4
  , EaF5
  , EaF6
  , PotM
  , PotF1
  , PotF2
  , PotF3
  , PotF4
  , PotF5
  , PotF6
  , Consumo_StD
  , EaF1_StD
  , EaF2_StD
  , EaF3_StD
  = Value
}

package it.au.misure.eng.schema

object GenericXmlSchema extends Enumeration {
  val FlussoMisure,
  IdentificativiFlusso,
  DatiPod,

  Pod,
  MeseAnno,
  DataMisura,
  CodPrat_SII,
  DataPrest,
  TipoRettifica,

  PotMax,
  Motivazione,
  Consumo,
  Forfait,

  DatiPdp,
  Trattamento,
  Ka,
  Kr,
  Kp,
  Raccolta,
  Ea,
  Er,

  Misura,
  MotivazioneStima,

  Curva,
  TipoDato,

  PIvaDistributore,
  PIvaUtente,
  CodContrDisp,
  Montaggio,
  Smontaggio,
  TipoMisuratore,
  EaM,
  PotM,
  DataMessaRegime2G,
  EaF1,
  EaF2,
  EaF3,
  PotF1,
  PotF2,
  PotF3
  = Value

  val CodFlusso: GenericXmlSchema.Value = Value("@CodFlusso")
  val Dst: GenericXmlSchema.Value = Value("@Dst")

  implicit def valueToString(value: Value) : String = value.toString

  def getValues: List[String] = this.values.toList.map(_.toString)
}

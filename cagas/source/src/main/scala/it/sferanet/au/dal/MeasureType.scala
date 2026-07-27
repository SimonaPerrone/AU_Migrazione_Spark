package it.sferanet.au.dal


object MeasureType extends Enumeration {
  type MeasureType = Value

  /** CR 22/08/2022 Gabrini Federico
   * 7.	autoletture: eliminare completamente le autoletture (TAL,TAS e TAV) dalla fase di ingestione.
   * */
  val
  A01,
  A01R,
  A02,
  A02R,
  A40,
  A40R,
  AD2,
  AD2R,
  AD3,
  AD3R,
  AD4,
  AD4R,
  AD5,
  AD5R,
  FDD,
  FUI,
  IGMGPOST,
  IGMGPRE,
  IGMRPOST,
  IGMRPRE,
  IM1POST,
  IM1PRE,
  M01,
  M01R,
  R01,
  R01R,
  R40,
  R40R,
  RGL,
  RML,
  RMV,
  RSL,
  S02,
  S02R,
  S40,
  S40R,
  SM1,
  SM1R,
  SW1,
  SWG1,
  TGL,
  TML,
  TMV,
  V01,
  V01R,
  V02,
  V02R
  = Value

}

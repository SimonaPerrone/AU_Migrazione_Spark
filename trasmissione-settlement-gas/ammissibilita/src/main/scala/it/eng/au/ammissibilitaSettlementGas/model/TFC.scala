package it.eng.au.ammissibilitaSettlementGas.model

case class TFC(
              fileName:String,
              yearDir: String,
              monthDir: String,
              numeroRiga:Int,
              fields:Array[String], // Fields of the csv file's line (e.g. 01/01/2011 | 11 | 0,2)
              data: Option[String],
              idRegClimatica: Option[String], // Sarà poi da mappare ad int, una volta effettuato il controllo apposito.
              wkr: Option[String],
              isAmmissibile: Boolean = true,
              statusCode: String = "",
              statusMessage: String = ""
              )

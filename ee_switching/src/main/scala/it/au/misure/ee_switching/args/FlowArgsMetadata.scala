package it.au.misure.ee_switching.args

case class FlowArgsMetadata(
                       flowName: String = null,
                       runOrdinaria: Boolean = true, // se true allora verranno considerate le date del giorno corrente
                       timestampFilePath: String = "",
                       listaPodFilePath: String = "",
                       listaDistributoriFilePath: String = "",
                       listaUddFilePath: String = "",
                       listaDateFunzionaliSWFilePath: String = "",
                       listaDateFunzionaliNAFilePath: String = "",
                       listaDateStoriciSWFilePath: String = "",
                       listaCoppieDistrUddFilePath: String = "",
                       queue: String = ""
                       ) {

  override def toString: String = s"FlowArgsMetadata=[" +
    s"flowName=$flowName, " +
    s"runOrdinaria=$runOrdinaria, " +
    s"timestampFilePath=$timestampFilePath, " +
    s"listaPodFilePath=$listaPodFilePath, " +
    s"listaDistributoriFilePath=$listaDistributoriFilePath, " +
    s"listaUddFilePath=$listaUddFilePath, " +
    s"listaDateFunzionaliSWFilePath=$listaDateFunzionaliSWFilePath, " +
    s"listaDateFunzionaliNAFilePath=$listaDateFunzionaliNAFilePath, " +
    s"listaDateStoriciSWFilePath=$listaDateStoriciSWFilePath, " +
    s"listaCoppieDistrUddFilePath=$listaCoppieDistrUddFilePath, " +
    s"queue=$queue" +
    s"]"

}

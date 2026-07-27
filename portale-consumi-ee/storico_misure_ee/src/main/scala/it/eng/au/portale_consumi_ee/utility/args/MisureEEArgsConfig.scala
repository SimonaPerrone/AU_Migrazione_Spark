package it.eng.au.portale_consumi_ee.utility.args
//todo set up properly in function of storic flow necessity
case class MisureEEArgsConfig(
                               flow: String = null, // Stage M or Load ML
                               storico: Boolean = false, //true 33 mesi, false 3 mesi
                               pathToProperties: String = null
                             ) {
  override def toString: String = {
    s"MisureEEArgsConfig=[" +
      s"flow=$flow" +
      s",storico=$storico" +
      s",path hdfs properties =$pathToProperties" +
      s"]"
  }
}

package it.au.misure.ee_switching.utility

import it.au.misure.ee_switching.model.schema.hive.{FunzionaliSchema, StoriciSchema}
import it.au.misure.ee_switching.model.schema.xml.StoriciXMLSchema
import it.au.misure.ee_switching.utility.environment.Environment

import scala.xml.{Elem, XML}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.{Assert, Test}

class TestFlowUtility extends EnvironmentSparkTest {

  var dfFunzionali: DataFrame = null
  var dfStorici: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext

    val rddFunzionali = sc.parallelize(List(
      Row("F2G", "202002", "XXX1234567890YYY", "12345678901", "DP9876", "000000000000000", "SII202001000000", "2020-02-01", "NORD", "G", "2019-10-01", "O", "380.000", "550.000",
        "480.000", "1.000", "0.899", "1.000", "aaabbbccccdddd111", "bbbcccvvvv1112222", "tttyyyuuuu1234567", "2019-12-12", "2019-12-12", "2019-12-12", "12", "12", "12", "SI", "SI", "aaa", "SI", "NO", "MT", "SI")
    ))

    val schemaFunzionali = StructType(Array(
      StructField(FunzionaliSchema.nome_flusso, StringType, nullable = true),
      StructField(FunzionaliSchema.annomese_sw, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_udd, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_distr, StringType, nullable = true),
      StructField(FunzionaliSchema.t_cod_contr_disp, StringType, nullable = true),
      StructField(FunzionaliSchema.pod14, StringType, nullable = true),
      StructField(FunzionaliSchema.t_protocollo, StringType, nullable = true),
      StructField(FunzionaliSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(FunzionaliSchema.t_area_rif, StringType, nullable = true),
      StructField(FunzionaliSchema.t_tipo_misuratore, StringType, nullable = true),
      StructField(FunzionaliSchema.d_regime, StringType, nullable = true),
      StructField(FunzionaliSchema.trattamento_online, StringType, nullable = true),
      StructField(FunzionaliSchema.n_tensione, StringType, nullable = true),
      StructField(FunzionaliSchema.n_potenza_impegnata, StringType, nullable = true),
      StructField(FunzionaliSchema.n_potenza_disponibile, StringType, nullable = true),
      StructField(FunzionaliSchema.n_k_trasfor_att, StringType, nullable = true),
      StructField(FunzionaliSchema.n_k_trasfor_rea, StringType, nullable = true),
      StructField(FunzionaliSchema.n_k_trasfor_pot, StringType, nullable = true),
      StructField(FunzionaliSchema.t_mat_misuratore_att, StringType, nullable = true),
      StructField(FunzionaliSchema.t_mat_misuratore_rea, StringType, nullable = true),
      StructField(FunzionaliSchema.t_mat_misuratore_pot, StringType, nullable = true),
      StructField(FunzionaliSchema.d_inst_misurator_att, StringType, nullable = true),
      StructField(FunzionaliSchema.d_inst_misurator_rea, StringType, nullable = true),
      StructField(FunzionaliSchema.d_inst_misurator_pot, StringType, nullable = true),
      StructField(FunzionaliSchema.n_num_cifre_att, StringType, nullable = true),
      StructField(FunzionaliSchema.n_num_cifre_rea, StringType, nullable = true),
      StructField(FunzionaliSchema.n_num_cifre_pot, StringType, nullable = true),
      StructField(FunzionaliSchema.b_presenza_mis, StringType, nullable = true),
      StructField(FunzionaliSchema.b_gest_forfait, StringType, nullable = true),
      StructField(FunzionaliSchema.t_tariffa_distr, StringType, nullable = true),
      StructField(FunzionaliSchema.t_residente, StringType, nullable = true),
      StructField(FunzionaliSchema.b_disalimentabilita, StringType, nullable = true),
      StructField(FunzionaliSchema.servizio_tutela, StringType, nullable = true),
      StructField(FunzionaliSchema.t_tipo_configurazione, StringType, nullable = true)
    ))

    dfFunzionali = sqlContext.createDataFrame(rddFunzionali, schemaFunzionali).cache


    val rddStorici = sc.parallelize(List(
      Row("S2G", "202002", "12345678901", "01234567890", "DP1234", "000000000000001", "01/01/2020", "SI", "F", "G", "E", "1000.000",
        """
          |<Ea E96="2,070" E95="2,310" E94="2,130" E93="1,920" E92="2,100" E91="2,220" E90="1,920" E89="1,980" E88="1,800" E87="1,800" E86="2,100" E85="1,800" E84="2,010" E83="2,250" E82="2,340" E81="3,060" E80="2,220" E79="2,070" E78="1,890" E77="1,680" E76="1,530" E75="1,890" E74="1,920" E73="1,770" E72="2,010" E71="1,740" E70="2,010" E69="2,010" E68="2,010" E67="1,710" E66="2,550" E65="1,860" E64="2,250" E63="2,160" E62="1,590" E61="1,680" E60="1,890" E59="1,200" E58="1,440" E57="1,440" E56="1,380" E55="1,440" E54="1,860" E53="1,560" E52="2,310" E51="2,490" E50="2,220" E49="1,380" E48="1,890" E47="2,280" E46="2,400" E45="2,190" E44="2,310" E43="1,620" E42="1,920" E41="1,890" E40="1,740" E39="1,530" E38="1,320" E37="1,980" E36="2,010" E35="1,380" E34="1,740" E33="1,500" E32="1,740" E31="1,350" E30="1,080" E29="1,230" E28="1,680" E27="1,530" E26="1,560" E25="1,590" E24="1,560" E23="1,590" E22="1,560" E21="1,590" E20="1,590" E19="1,590" E18="1,590" E17="1,860" E16="1,620" E15="1,620" E14="1,620" E13="1,650" E12="1,620" E11="1,590" E10="1,620" E9="1,590" E8="1,590" E7="1,860" E6="1,590" E5="1,620" E4="1,620" E3="1,680" E2="2,460" E1="1,740">01</Ea>
          |<Ea E96="2,070" E95="2,310" E94="2,130" E93="1,920" E92="2,100" E91="2,220" E90="1,920" E89="1,980" E88="1,800" E87="1,800" E86="2,100" E85="1,800" E84="2,010" E83="2,250" E82="2,340" E81="3,060" E80="2,220" E79="2,070" E78="1,890" E77="1,680" E76="1,530" E75="1,890" E74="1,920" E73="1,770" E72="2,010" E71="1,740" E70="2,010" E69="2,010" E68="2,010" E67="1,710" E66="2,550" E65="1,860" E64="2,250" E63="2,160" E62="1,590" E61="1,680" E60="1,890" E59="1,200" E58="1,440" E57="1,440" E56="1,380" E55="1,440" E54="1,860" E53="1,560" E52="2,310" E51="2,490" E50="2,220" E49="1,380" E48="1,890" E47="2,280" E46="2,400" E45="2,190" E44="2,310" E43="1,620" E42="1,920" E41="1,890" E40="1,740" E39="1,530" E38="1,320" E37="1,980" E36="2,010" E35="1,380" E34="1,740" E33="1,500" E32="1,740" E31="1,350" E30="1,080" E29="1,230" E28="1,680" E27="1,530" E26="1,560" E25="1,590" E24="1,560" E23="1,590" E22="1,560" E21="1,590" E20="1,590" E19="1,590" E18="1,590" E17="1,860" E16="1,620" E15="1,620" E14="1,620" E13="1,650" E12="1,620" E11="1,590" E10="1,620" E9="1,590" E8="1,590" E7="1,860" E6="1,590" E5="1,620" E4="1,620" E3="1,680" E2="2,460" E1="1,740">02</Ea>
          |""".stripMargin,
        "2.3453333333333", "2.456", "4.098", "5.098", "6.987", "2.098", "10.000", "34.000", "9.000", "5.000", "10.000", "500.000", null, "null", null, "NO"),
      Row("S2G", "202002", "12345678901", "01234567890", "DP1234", "000000000000002", "01/01/2020", "SI", "F", "G", "E", "1000.000",
        """
          |<Ea E96="2,070" E95="2,310" E94="2,130" E93="1,920" E92="2,100" E91="2,220" E90="1,920" E89="1,980" E88="1,800" E87="1,800" E86="2,100" E85="1,800" E84="2,010" E83="2,250" E82="2,340" E81="3,060" E80="2,220" E79="2,070" E78="1,890" E77="1,680" E76="1,530" E75="1,890" E74="1,920" E73="1,770" E72="2,010" E71="1,740" E70="2,010" E69="2,010" E68="2,010" E67="1,710" E66="2,550" E65="1,860" E64="2,250" E63="2,160" E62="1,590" E61="1,680" E60="1,890" E59="1,200" E58="1,440" E57="1,440" E56="1,380" E55="1,440" E54="1,860" E53="1,560" E52="2,310" E51="2,490" E50="2,220" E49="1,380" E48="1,890" E47="2,280" E46="2,400" E45="2,190" E44="2,310" E43="1,620" E42="1,920" E41="1,890" E40="1,740" E39="1,530" E38="1,320" E37="1,980" E36="2,010" E35="1,380" E34="1,740" E33="1,500" E32="1,740" E31="1,350" E30="1,080" E29="1,230" E28="1,680" E27="1,530" E26="1,560" E25="1,590" E24="1,560" E23="1,590" E22="1,560" E21="1,590" E20="1,590" E19="1,590" E18="1,590" E17="1,860" E16="1,620" E15="1,620" E14="1,620" E13="1,650" E12="1,620" E11="1,590" E10="1,620" E9="1,590" E8="1,590" E7="1,860" E6="1,590" E5="1,620" E4="1,620" E3="1,680" E2="2,460" E1="1,740">01</Ea>
          |""".stripMargin,
        "-2.345", "2.456", "4.098", "5.098", "6.987", "2.098", "10.000", "34.000", "9.000", "5.000", "10.000", "500.000", "null", "null", "null", "NO"),
      Row("S2G", "202002", "12345678901", "01234567890", "DP1234", "000000000000003", "01/01/2020", "SI", "O", "G", "E", "1000.000",
        """
          |<Ea E96="2,070" E95="2,310" E94="2,130" E93="1,920" E92="2,100" E91="2,220" E90="1,920" E89="1,980" E88="1,800" E87="1,800" E86="2,100" E85="1,800" E84="2,010" E83="2,250" E82="2,340" E81="3,060" E80="2,220" E79="2,070" E78="1,890" E77="1,680" E76="1,530" E75="1,890" E74="1,920" E73="1,770" E72="2,010" E71="1,740" E70="2,010" E69="2,010" E68="2,010" E67="1,710" E66="2,550" E65="1,860" E64="2,250" E63="2,160" E62="1,590" E61="1,680" E60="1,890" E59="1,200" E58="1,440" E57="1,440" E56="1,380" E55="1,440" E54="1,860" E53="1,560" E52="2,310" E51="2,490" E50="2,220" E49="1,380" E48="1,890" E47="2,280" E46="2,400" E45="2,190" E44="2,310" E43="1,620" E42="1,920" E41="1,890" E40="1,740" E39="1,530" E38="1,320" E37="1,980" E36="2,010" E35="1,380" E34="1,740" E33="1,500" E32="1,740" E31="1,350" E30="1,080" E29="1,230" E28="1,680" E27="1,530" E26="1,560" E25="1,590" E24="1,560" E23="1,590" E22="1,560" E21="1,590" E20="1,590" E19="1,590" E18="1,590" E17="1,860" E16="1,620" E15="1,620" E14="1,620" E13="1,650" E12="1,620" E11="1,590" E10="1,620" E9="1,590" E8="1,590" E7="1,860" E6="1,590" E5="1,620" E4="1,620" E3="1,680" E2="2,460" E1="1,740">01</Ea>
          |<Ea E96="2,070" E95="2,310" E94="2,130" E93="1,920" E92="2,100" E91="2,220" E90="1,920" E89="1,980" E88="1,800" E87="1,800" E86="2,100" E85="1,800" E84="2,010" E83="2,250" E82="2,340" E81="3,060" E80="2,220" E79="2,070" E78="1,890" E77="1,680" E76="1,530" E75="1,890" E74="1,920" E73="1,770" E72="2,010" E71="1,740" E70="2,010" E69="2,010" E68="2,010" E67="1,710" E66="2,550" E65="1,860" E64="2,250" E63="2,160" E62="1,590" E61="1,680" E60="1,890" E59="1,200" E58="1,440" E57="1,440" E56="1,380" E55="1,440" E54="1,860" E53="1,560" E52="2,310" E51="2,490" E50="2,220" E49="1,380" E48="1,890" E47="2,280" E46="2,400" E45="2,190" E44="2,310" E43="1,620" E42="1,920" E41="1,890" E40="1,740" E39="1,530" E38="1,320" E37="1,980" E36="2,010" E35="1,380" E34="1,740" E33="1,500" E32="1,740" E31="1,350" E30="1,080" E29="1,230" E28="1,680" E27="1,530" E26="1,560" E25="1,590" E24="1,560" E23="1,590" E22="1,560" E21="1,590" E20="1,590" E19="1,590" E18="1,590" E17="1,860" E16="1,620" E15="1,620" E14="1,620" E13="1,650" E12="1,620" E11="1,590" E10="1,620" E9="1,590" E8="1,590" E7="1,860" E6="1,590" E5="1,620" E4="1,620" E3="1,680" E2="2,460" E1="1,740">02</Ea>
          |""".stripMargin,
        "2.3453333333333", "2.456", "4.098", "5.098", "6.987", "2.098", "10.000", "34.000", "9.000", "5.000", "10.000", "500.000", "10.0", "10", "10", "SI")
    ))

    val schemaStorici = StructType(Array(
      StructField(StoriciSchema.nome_flusso, StringType, nullable = true),
      StructField(StoriciSchema.annomese_sw, StringType, nullable = true),
      StructField(StoriciSchema.piva_udd, StringType, nullable = true),
      StructField(StoriciSchema.piva_distr, StringType, nullable = true),
      StructField(StoriciSchema.dp, StringType, nullable = true),
      StructField(StoriciSchema.pod14, StringType, nullable = true),
      StructField(StoriciSchema.data_misura, StringType, nullable = true),
      StructField(StoriciSchema.messa_regime, StringType, nullable = true),
      StructField(StoriciSchema.trattamento, StringType, nullable = true),
      StructField(StoriciSchema.tipo_misuratore, StringType, nullable = true),
      StructField(StoriciSchema.tipo_dato, StringType, nullable = true),
      StructField(StoriciSchema.last_potmax, StringType, nullable = true),
      StructField(StoriciXMLSchema.Ea, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf1, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf2, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf3, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf4, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf5, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf6, StringType, nullable = true),
      StructField(StoriciSchema.last_potf1, StringType, nullable = true),
      StructField(StoriciSchema.last_potf2, StringType, nullable = true),
      StructField(StoriciSchema.last_potf3, StringType, nullable = true),
      StructField(StoriciSchema.last_potf4, StringType, nullable = true),
      StructField(StoriciSchema.last_potf5, StringType, nullable = true),
      StructField(StoriciSchema.last_potf6, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf1_riconf, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf2_riconf, StringType, nullable = true),
      StructField(StoriciSchema.somma_eaf3_riconf, StringType, nullable = true),
      StructField(StoriciSchema.t_tipo_configurazione, StringType, nullable = true)
    ))

    dfStorici = sqlContext.createDataFrame(rddStorici, schemaStorici).cache
  }

  @Test
  def testBuildFunzionaliDatiPodXmlNode(): Unit = {

    val printer = new scala.xml.PrettyPrinter(1000000, 2)

    val datiPodXmlNode1: Elem = <DatiPod>
    <Pod>000000000000000</Pod>
      <CodPrat_SII>SII202001000000</CodPrat_SII>
      <DataInizio>01/02/2020</DataInizio>
      <DatiPdp>
        <PuntoDispacciamento>NORD</PuntoDispacciamento>
        <TipoMisuratore>G</TipoMisuratore>
        <DataMessaRegime2G>01/10/2019</DataMessaRegime2G>
        <Trattamento>O</Trattamento>
        <Tensione>380</Tensione>
        <PotImp>550,000</PotImp>
        <PotDisp>480,000</PotDisp>
        <Ka>1,000</Ka>
        <Kr>0,899</Kr>
        <Kp>1,000</Kp>
        <MatrAtt>aaabbbccccdddd111</MatrAtt>
        <MatrRea>bbbcccvvvv1112222</MatrRea>
        <MatrPot>tttyyyuuuu1234567</MatrPot>
        <DataInstMisAtt>12/12/2019</DataInstMisAtt>
        <DataInstMisRea>12/12/2019</DataInstMisRea>
        <DataInstMisPot>12/12/2019</DataInstMisPot>
        <CifreAtt>12</CifreAtt>
        <CifreRea>12</CifreRea>
        <CifrePot>12</CifrePot>
        <GruppoMis>SI</GruppoMis>
        <Forfait>SI</Forfait>
        <CodiceTariffa>aaa</CodiceTariffa>
        <Residenza>SI</Residenza>
        <Disaliment>NO</Disaliment>
        <ServizioTutela>MT</ServizioTutela>
        <ConfMis>SI</ConfMis>
      </DatiPdp>
    </DatiPod>

    Assert.assertEquals(printer.format(datiPodXmlNode1), printer.format(XML.loadString(FlowUtility.buildFunzionaliDatiPodXmlNode(dfFunzionali.rdd.filter(row => row.getAs[String](FunzionaliSchema.pod14).equals("000000000000000")).take(1).head)._2)))

  }


  @Test
  def testBuildStoriciDatiPodXmlNode(): Unit = {

    val printer = new scala.xml.PrettyPrinter(1000000, 2)

//    dfStorici.rdd.foreach(row => println(FlowUtility.buildStoriciDatiPodXmlNode(row)._2._2))
//    dfStorici.rdd.foreach(row => println(XML.loadString(FlowUtility.buildStoriciDatiPodXmlNode(row)._2._2)))

    val datiPodXmlNode1: Elem = <DatiPod>
      <Pod>000000000000001</Pod>
      <MeseAnno>01/2020</MeseAnno>
      <DatiPdp>
        <MessaRegime>SI</MessaRegime>
        <Trattamento>F</Trattamento>
        <TipoMisuratore>G</TipoMisuratore>
      </DatiPdp>
      <Consumo>
        <TipoDato>E</TipoDato>
        <PotMax>1000,000</PotMax>
        <Ea E1="1,740" E2="2,460" E3="1,680" E4="1,620" E5="1,620" E6="1,590" E7="1,860" E8="1,590" E9="1,590" E10="1,620" E11="1,590" E12="1,620" E13="1,650" E14="1,620" E15="1,620" E16="1,620" E17="1,860" E18="1,590" E19="1,590" E20="1,590" E21="1,590" E22="1,560" E23="1,590" E24="1,560" E25="1,590" E26="1,560" E27="1,530" E28="1,680" E29="1,230" E30="1,080" E31="1,350" E32="1,740" E33="1,500" E34="1,740" E35="1,380" E36="2,010" E37="1,980" E38="1,320" E39="1,530" E40="1,740" E41="1,890" E42="1,920" E43="1,620" E44="2,310" E45="2,190" E46="2,400" E47="2,280" E48="1,890" E49="1,380" E50="2,220" E51="2,490" E52="2,310" E53="1,560" E54="1,860" E55="1,440" E56="1,380" E57="1,440" E58="1,440" E59="1,200" E60="1,890" E61="1,680" E62="1,590" E63="2,160" E64="2,250" E65="1,860" E66="2,550" E67="1,710" E68="2,010" E69="2,010" E70="2,010" E71="1,740" E72="2,010" E73="1,770" E74="1,920" E75="1,890" E76="1,530" E77="1,680" E78="1,890" E79="2,070" E80="2,220" E81="3,060" E82="2,340" E83="2,250" E84="2,010" E85="1,800" E86="2,100" E87="1,800" E88="1,800" E89="1,980" E90="1,920" E91="2,220" E92="2,100" E93="1,920" E94="2,130" E95="2,310" E96="2,070">01</Ea>
        <Ea E1="1,740" E2="2,460" E3="1,680" E4="1,620" E5="1,620" E6="1,590" E7="1,860" E8="1,590" E9="1,590" E10="1,620" E11="1,590" E12="1,620" E13="1,650" E14="1,620" E15="1,620" E16="1,620" E17="1,860" E18="1,590" E19="1,590" E20="1,590" E21="1,590" E22="1,560" E23="1,590" E24="1,560" E25="1,590" E26="1,560" E27="1,530" E28="1,680" E29="1,230" E30="1,080" E31="1,350" E32="1,740" E33="1,500" E34="1,740" E35="1,380" E36="2,010" E37="1,980" E38="1,320" E39="1,530" E40="1,740" E41="1,890" E42="1,920" E43="1,620" E44="2,310" E45="2,190" E46="2,400" E47="2,280" E48="1,890" E49="1,380" E50="2,220" E51="2,490" E52="2,310" E53="1,560" E54="1,860" E55="1,440" E56="1,380" E57="1,440" E58="1,440" E59="1,200" E60="1,890" E61="1,680" E62="1,590" E63="2,160" E64="2,250" E65="1,860" E66="2,550" E67="1,710" E68="2,010" E69="2,010" E70="2,010" E71="1,740" E72="2,010" E73="1,770" E74="1,920" E75="1,890" E76="1,530" E77="1,680" E78="1,890" E79="2,070" E80="2,220" E81="3,060" E82="2,340" E83="2,250" E84="2,010" E85="1,800" E86="2,100" E87="1,800" E88="1,800" E89="1,980" E90="1,920" E91="2,220" E92="2,100" E93="1,920" E94="2,130" E95="2,310" E96="2,070">02</Ea>
        <EaF1>2,345</EaF1>
        <EaF2>2,456</EaF2>
        <EaF3>4,098</EaF3>
        <EaF4>5,098</EaF4>
        <EaF5>6,987</EaF5>
        <EaF6>2,098</EaF6>
        <PotF1>10,000</PotF1>
        <PotF2>34,000</PotF2>
        <PotF3>9,000</PotF3>
        <PotF4>5,000</PotF4>
        <PotF5>10,000</PotF5>
        <PotF6>500,000</PotF6>
      </Consumo>
    </DatiPod>

    Assert.assertEquals(printer.format(datiPodXmlNode1), printer.format(XML.loadString(FlowUtility.buildStoriciDatiPodXmlNode(dfStorici.rdd.filter(row => row.getAs[String](StoriciSchema.pod14).equals("000000000000001")).take(1).head)._2)))


    val datiPodXmlNode2: Elem = <DatiPod>
      <Pod>000000000000002</Pod>
      <MeseAnno>01/2020</MeseAnno>
      <DatiPdp>
        <MessaRegime>SI</MessaRegime>
        <Trattamento>F</Trattamento>
        <TipoMisuratore>G</TipoMisuratore>
      </DatiPdp>
      <Consumo>
        <TipoDato>E</TipoDato>
        <PotMax>1000,000</PotMax>
        <Ea E1="1,740" E2="2,460" E3="1,680" E4="1,620" E5="1,620" E6="1,590" E7="1,860" E8="1,590" E9="1,590" E10="1,620" E11="1,590" E12="1,620" E13="1,650" E14="1,620" E15="1,620" E16="1,620" E17="1,860" E18="1,590" E19="1,590" E20="1,590" E21="1,590" E22="1,560" E23="1,590" E24="1,560" E25="1,590" E26="1,560" E27="1,530" E28="1,680" E29="1,230" E30="1,080" E31="1,350" E32="1,740" E33="1,500" E34="1,740" E35="1,380" E36="2,010" E37="1,980" E38="1,320" E39="1,530" E40="1,740" E41="1,890" E42="1,920" E43="1,620" E44="2,310" E45="2,190" E46="2,400" E47="2,280" E48="1,890" E49="1,380" E50="2,220" E51="2,490" E52="2,310" E53="1,560" E54="1,860" E55="1,440" E56="1,380" E57="1,440" E58="1,440" E59="1,200" E60="1,890" E61="1,680" E62="1,590" E63="2,160" E64="2,250" E65="1,860" E66="2,550" E67="1,710" E68="2,010" E69="2,010" E70="2,010" E71="1,740" E72="2,010" E73="1,770" E74="1,920" E75="1,890" E76="1,530" E77="1,680" E78="1,890" E79="2,070" E80="2,220" E81="3,060" E82="2,340" E83="2,250" E84="2,010" E85="1,800" E86="2,100" E87="1,800" E88="1,800" E89="1,980" E90="1,920" E91="2,220" E92="2,100" E93="1,920" E94="2,130" E95="2,310" E96="2,070">01</Ea>
        <EaF1/>
        <EaF2>2,456</EaF2>
        <EaF3>4,098</EaF3>
        <EaF4>5,098</EaF4>
        <EaF5>6,987</EaF5>
        <EaF6>2,098</EaF6>
        <PotF1>10,000</PotF1>
        <PotF2>34,000</PotF2>
        <PotF3>9,000</PotF3>
        <PotF4>5,000</PotF4>
        <PotF5>10,000</PotF5>
        <PotF6>500,000</PotF6>
      </Consumo>
    </DatiPod>

    Assert.assertEquals(printer.format(datiPodXmlNode2), printer.format(XML.loadString(FlowUtility.buildStoriciDatiPodXmlNode(dfStorici.rdd.filter(row => row.getAs[String](StoriciSchema.pod14).equals("000000000000002")).take(1).head)._2)))


    val datiPodXmlNode3: Elem = <DatiPod>
      <Pod>000000000000003</Pod>
      <MeseAnno>01/2020</MeseAnno>
      <DatiPdp>
        <MessaRegime>SI</MessaRegime>
        <Trattamento>O</Trattamento>
        <TipoMisuratore>G</TipoMisuratore>
      </DatiPdp>
      <Consumo>
        <TipoDato>E</TipoDato>
        <PotMax>1000,000</PotMax>
        <Ea E1="1,740" E2="2,460" E3="1,680" E4="1,620" E5="1,620" E6="1,590" E7="1,860" E8="1,590" E9="1,590" E10="1,620" E11="1,590" E12="1,620" E13="1,650" E14="1,620" E15="1,620" E16="1,620" E17="1,860" E18="1,590" E19="1,590" E20="1,590" E21="1,590" E22="1,560" E23="1,590" E24="1,560" E25="1,590" E26="1,560" E27="1,530" E28="1,680" E29="1,230" E30="1,080" E31="1,350" E32="1,740" E33="1,500" E34="1,740" E35="1,380" E36="2,010" E37="1,980" E38="1,320" E39="1,530" E40="1,740" E41="1,890" E42="1,920" E43="1,620" E44="2,310" E45="2,190" E46="2,400" E47="2,280" E48="1,890" E49="1,380" E50="2,220" E51="2,490" E52="2,310" E53="1,560" E54="1,860" E55="1,440" E56="1,380" E57="1,440" E58="1,440" E59="1,200" E60="1,890" E61="1,680" E62="1,590" E63="2,160" E64="2,250" E65="1,860" E66="2,550" E67="1,710" E68="2,010" E69="2,010" E70="2,010" E71="1,740" E72="2,010" E73="1,770" E74="1,920" E75="1,890" E76="1,530" E77="1,680" E78="1,890" E79="2,070" E80="2,220" E81="3,060" E82="2,340" E83="2,250" E84="2,010" E85="1,800" E86="2,100" E87="1,800" E88="1,800" E89="1,980" E90="1,920" E91="2,220" E92="2,100" E93="1,920" E94="2,130" E95="2,310" E96="2,070">01</Ea>
        <Ea E1="1,740" E2="2,460" E3="1,680" E4="1,620" E5="1,620" E6="1,590" E7="1,860" E8="1,590" E9="1,590" E10="1,620" E11="1,590" E12="1,620" E13="1,650" E14="1,620" E15="1,620" E16="1,620" E17="1,860" E18="1,590" E19="1,590" E20="1,590" E21="1,590" E22="1,560" E23="1,590" E24="1,560" E25="1,590" E26="1,560" E27="1,530" E28="1,680" E29="1,230" E30="1,080" E31="1,350" E32="1,740" E33="1,500" E34="1,740" E35="1,380" E36="2,010" E37="1,980" E38="1,320" E39="1,530" E40="1,740" E41="1,890" E42="1,920" E43="1,620" E44="2,310" E45="2,190" E46="2,400" E47="2,280" E48="1,890" E49="1,380" E50="2,220" E51="2,490" E52="2,310" E53="1,560" E54="1,860" E55="1,440" E56="1,380" E57="1,440" E58="1,440" E59="1,200" E60="1,890" E61="1,680" E62="1,590" E63="2,160" E64="2,250" E65="1,860" E66="2,550" E67="1,710" E68="2,010" E69="2,010" E70="2,010" E71="1,740" E72="2,010" E73="1,770" E74="1,920" E75="1,890" E76="1,530" E77="1,680" E78="1,890" E79="2,070" E80="2,220" E81="3,060" E82="2,340" E83="2,250" E84="2,010" E85="1,800" E86="2,100" E87="1,800" E88="1,800" E89="1,980" E90="1,920" E91="2,220" E92="2,100" E93="1,920" E94="2,130" E95="2,310" E96="2,070">02</Ea>
        <EaF1>2,345</EaF1>
        <EaF2>2,456</EaF2>
        <EaF3>4,098</EaF3>
        <EaF4>5,098</EaF4>
        <EaF5>6,987</EaF5>
        <EaF6>2,098</EaF6>
        <PotF1>10,000</PotF1>
        <PotF2>34,000</PotF2>
        <PotF3>9,000</PotF3>
        <PotF4>5,000</PotF4>
        <PotF5>10,000</PotF5>
        <PotF6>500,000</PotF6>
      </Consumo>
      <Consumo_StD>
        <EaF1_StD>10,000</EaF1_StD>
        <EaF2_StD>10,000</EaF2_StD>
        <EaF3_StD>10,000</EaF3_StD>
      </Consumo_StD>
    </DatiPod>

    Assert.assertEquals(printer.format(datiPodXmlNode3), printer.format(XML.loadString(FlowUtility.buildStoriciDatiPodXmlNode(dfStorici.rdd.filter(row => row.getAs[String](StoriciSchema.pod14).equals("000000000000003")).take(1).head)._2)))

  }


}

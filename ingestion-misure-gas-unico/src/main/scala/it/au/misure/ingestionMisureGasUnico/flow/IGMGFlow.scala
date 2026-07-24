package it.au.misure.ingestionMisureGasUnico.flow

import it.au.misure.ingestionMisureGasUnico.model.schema.CommonColumnsSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMGSchema.annomese
import it.au.misure.ingestionMisureGasUnico.model.schema.IGMGXMLSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema._
import it.au.misure.ingestionMisureGasUnico.model.schema.rcu.{RcuGasPdrSchema, RcuGasPdrStatoSchema}
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoPDRMessage
import it.au.misure.ingestionMisureGasUnico.model.{GasMetadata, GasXmlMetadata}
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.utility.{DataFrameUtility, DateTimeUtility, FileUtility, PropertyUtility}
import it.au.misure.ingestionMisureGasUnico.validate.CheckAmmissibilitaPDRRulesIGMG
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try
import scala.xml.{Node, XML}

object IGMGFlow extends Flow {
  override val schema: SchemaEnum = IGMGSchema
  val exportSchema: SchemaEnum = IGMGExportSchema
  override val hiveTableName: String = "prt_cmg_igmg_p"
  val hiveExportTableName: String = "prt_cmg_igmg_export"
  override val flowType: String = "IGMG"

  override def flowName: String = "IGMG"

  override val flowDataPath: String = s"$rootPath/$flowType/$flowName"
  override val partitioningColumns: List[String] = List(
    annomese
  )
  override val ammissPath: String = PropertyUtility.getAmmissibilitaIgmgPath

  override def run(unzipTimestamp: String)/*(implicit sc: SparkContext, SQLContext: SQLContext)*/: Unit = {
    cleanOldTmpReportFiles(flowName) // fondamentale che la pulizia sia eseguita prima del prossimo if
    if (checkInputPathFiles > 0) {
      val inputRdd = loadData
      val (validationInfoRdd, validatedRdd) = validate(inputRdd)
      val inputDf = parse(validatedRdd)
      val fullDf = addCommonColumns(inputDf, unzipTimestamp)
      fullDf.persist(StorageLevel.MEMORY_AND_DISK)
      write(fullDf)
      writeExport(fullDf)
      writeReport(fullDf)
      writeValidationReports(validationInfoRdd, unzipTimestamp)
      deleteData()
    } else {
      logger.warn(s"$flowDataPath does not have XML files.")
    }
  }

  def parseXml(gasMetadata: GasMetadata): List[Row] = {
    val localRootPath = PropertyUtility.getUnzipInputPath
    val xml = gasMetadata.loadXml
    val localFile = s"$localRootPath/${gasMetadata.originalRelativePath}"

    val codFlusso = (xml \ cod_flusso).text
    val pivaUtente = (xml \ IdentificativiFlusso \ piva_utente).text
    val pivaDistr = (xml \ IdentificativiFlusso \ piva_distr).text

    (xml \ DatiPdR).toList.map(pdrNode => {
      val codPdr = (pdrNode \ cod_PdR).text
      val cauIntMis = FileUtility.extractNodeOrNull(pdrNode \ cau_int_mis)
      val cauIntCor = FileUtility.extractNodeOrNull(pdrNode \ cau_int_cor)
      val dataMisura = (pdrNode \ data_misura).text
      val causaOstativa = FileUtility.extractNodeOrNull(pdrNode \ causa_ostativa)

      val matrMisPreInt = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ matr_mis)
      val preConvPreInt = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ pre_conv)
      val matrConvPreInt = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ matr_conv)
      val coeffCorrPreInt = (pdrNode \ Pre_int \ coeff_corr).text
      val gruppoMisIntPreInt = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ gruppo_mis_int)
      val letMisuratorePreInt = (pdrNode \ Pre_int \ let_misuratore).text
      val letCorrettorePreInt = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ let_correttore)
      val tipoLet = (pdrNode \ Pre_int \ tipo_let).text
      val rinunciaVerifica = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ rinuncia_verifica)
      val causaStima = FileUtility.extractNodeOrNull(pdrNode \ Pre_int \ causa_stima)

      val matrMisPostInt = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ matr_mis)
      val classeGruppoMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ classe_gruppo_mis)
      val tipoMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ tipo_mis)
      val telegestione = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ IGMGXMLSchema.telegestione)
      val preConvPostInt = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ pre_conv)
      val matrConvPostInt = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ matr_conv)
      val nCifreConv = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ n_cifre_conv)
      val annoFabConv = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ anno_fabb_conv)
      val dataInstConv = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ data_inst_conv)
      val coeffCorrPostInt = (pdrNode \ Post_int \ coeff_corr).text
      val pressMisura = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ press_misura)
      val accMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ acc_mis)
      val nCifreMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ n_cifre_mis)
      val annoFabMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ anno_fabb_mis)
      val dataInstMis = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ data_inst_mis)
      val gruppoMisIntPostInt = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ gruppo_mis_int)
      val letMisuratorePostInt = (pdrNode \ Post_int \ let_misuratore).text
      val letCorrettorePostInt = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ let_correttore)
      val dataInservizioSm = FileUtility.extractNodeOrNull(pdrNode \ Post_int \ data_inservizio_sm)
      Row(
        codFlusso
        , pivaUtente
        , pivaDistr
        , codPdr
        , cauIntMis
        , cauIntCor
        , dataMisura
        , causaOstativa
        , matrMisPreInt
        , preConvPreInt
        , matrConvPreInt
        , coeffCorrPreInt
        , gruppoMisIntPreInt
        , letMisuratorePreInt
        , letCorrettorePreInt
        , tipoLet
        , rinunciaVerifica
        , causaStima
        , matrMisPostInt
        , classeGruppoMis
        , tipoMis
        , telegestione
        , preConvPostInt
        , matrConvPostInt
        , nCifreConv
        , annoFabConv
        , dataInstConv
        , coeffCorrPostInt
        , pressMisura
        , accMis
        , nCifreMis
        , annoFabMis
        , dataInstMis
        , gruppoMisIntPostInt
        , letMisuratorePostInt
        , letCorrettorePostInt
        , dataInservizioSm
        , gasMetadata.getAmmissibilita(codPdr)
        , localFile
        , gasMetadata.anno
        , gasMetadata.annoRiferimento
        , gasMetadata.mese
        , gasMetadata.meseRiferimento
        , gasMetadata.giorno
        , new File(gasMetadata.originalRelativePath).getName
      )
    })
  }

  def validate(inputRdd: RDD[GasXmlMetadata])
              /*(implicit sc: SparkContext, sqlContext: SQLContext)*/: (RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], RDD[GasXmlMetadata]) = {
    val checkAmm = Environment.getSpark.sparkContext.broadcast(new CheckAmmissibilitaPDRRulesIGMG)

    val pdrWithMetaRdd = inputRdd.flatMap(gasXmlMetada => {
       (gasXmlMetada.xmlNode \\ IGMGXMLSchema.FlussoIGMG \\ IGMGXMLSchema.DatiPdR).toList
        .map(datiPdr => (datiPdr,gasXmlMetada))
    })

    val pdrWithExtraMetaRdd = getPdrExtraMetadata(pdrWithMetaRdd)

    val xmlWithMessages = pdrWithExtraMetaRdd
      .map({case(pdrNode, gasXmlMetada) => (gasXmlMetada.file.getPath,( gasXmlMetada, List(checkAmm.value.check(pdrNode, gasXmlMetada)) ) )})
      .reduceByKey({case((meta1, message1), (meta2, message2))=> (meta1, message1++message2)})
      .map({ case(fileName,(meta,messageList)) => (meta,messageList) })
      .persist(StorageLevel.MEMORY_AND_DISK)

    val outputRddData = xmlWithMessages.map({ case (gasXmlMetada, messages) =>
      val ammissibilitaMap = messages.map(message => (message.pdr, message.bloccante)).toMap
      gasXmlMetada.copy(ammissibilita = ammissibilitaMap)
    })

    (xmlWithMessages, outputRddData)
  }

  override def parse(inputRdd: RDD[GasXmlMetadata])
                    /*(implicit sc: SparkContext, sqlContext: SQLContext)*/: DataFrame = {
    val dfRdd = inputRdd.flatMap(parseXml)
    Environment.getSpark.sqlContext.createDataFrame(dfRdd, IGMGParsedSchema.createSparkSchema())
  }

  override def addCommonColumns(df: DataFrame, unzipTimestamp: String): DataFrame = {
    val commonDf = super.addCommonColumns(df, unzipTimestamp)
      .withColumn(annomese,
        from_unixtime(unix_timestamp(df(data_misura), ITALIAN_DATE_PATTERN), ANNOMESE_PATTERN))

    commonDf
      .na.fill(EE.toString, List(annomese.toString))
  }

  def writeExport(df: DataFrame): Unit = {
    val exportDf = df
      .where(col(IGMGExportSchema.ammissibilita) =!= BLOCCANTE)
      .withColumn(data_misura, from_unixtime(unix_timestamp(df(data_misura), ITALIAN_DATE_PATTERN), JDBC_TIMESTAMP_PATTERN))
      .withColumn(data_inst_mis, from_unixtime(unix_timestamp(df(data_inst_mis), ITALIAN_DATE_PATTERN), JDBC_TIMESTAMP_PATTERN))
      .withColumn(data_inst_conv, from_unixtime(unix_timestamp(df(data_inst_conv), ITALIAN_DATE_PATTERN), JDBC_TIMESTAMP_PATTERN))
      .withColumn(data_inservizio_sm, from_unixtime(unix_timestamp(df(data_inservizio_sm), ITALIAN_DATE_PATTERN), JDBC_TIMESTAMP_PATTERN))

    // trim colonne (sono tutte String)
    val trimmedDf = DataFrameUtility.trimColumns(exportDf, exportSchema.getValues)

    logger.info(s"Writing df to $hiveDatabaseName.$hiveExportTableName")
    trimmedDf
      .selectExpr(exportSchema.getValues: _*)
      .write
      .mode(SaveMode.Append)
      .insertInto(s"$hiveDatabaseName.$hiveExportTableName")
    logger.info(s"The df was written to $hiveDatabaseName.$hiveExportTableName")
  }

  def getPdrExtraMetadata(rddPdrMeta:RDD[(Node, GasXmlMetadata)])/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: RDD[(Node, GasXmlMetadata)] = {
    //1. take in input DatiPdr and XML metadata
    //2-3. enrich those metadata with extra values from rcugas_pdr_p and rcugas_pdr_stato_p
    //4. return RDD(datiPdrNode,Metadata) to continue with validation process

    val rcuGasPdrRdd = Environment.getSpark.sqlContext.table(PropertyUtility.getRcugasPdrTable)
      .select(RcuGasPdrSchema.t_codice_pdr,RcuGasPdrSchema.n_id_pdr)
      .rdd
      .map(row => (row.getAs[String](RcuGasPdrSchema.t_codice_pdr), row.getAs[String](RcuGasPdrSchema.n_id_pdr) ))

    val rcuGasPdrStatoRdd = Environment.getSpark.sqlContext.table(PropertyUtility.getRcugasPdrStatoTable)
      .select(RcuGasPdrStatoSchema.n_id_pdr,RcuGasPdrStatoSchema.d_data_inizio,RcuGasPdrStatoSchema.d_data_fine, RcuGasPdrStatoSchema.t_cod_stato_pdr)
      .na.fill(LocalDateTime.MIN.format(DateTimeFormatter.ofPattern(RCUGAS_PDR_STATO_DATE_TIME_PATTERN)),Array(RcuGasPdrStatoSchema.d_data_inizio.toString))
      .na.fill(LocalDateTime.MAX.format(DateTimeFormatter.ofPattern(RCUGAS_PDR_STATO_DATE_TIME_PATTERN)),Array(RcuGasPdrStatoSchema.d_data_fine.toString))
      .rdd
      .map(row=>(row.getAs[String](RcuGasPdrStatoSchema.n_id_pdr),(row.getAs[String](RcuGasPdrStatoSchema.d_data_inizio),row.getAs[String](RcuGasPdrStatoSchema.d_data_fine),row.getAs[String](RcuGasPdrStatoSchema.t_cod_stato_pdr))))

    // 1. prepare input rdd for join using codPdr as key -> create RDD[ (codPdr,(pdrNode,gasMetadata)) ]
    // 1.1 setting metadata.xmlNode = null to save space during shuffles
    val rdd = rddPdrMeta.map({case (pdrNode, meta) =>((pdrNode\cod_PdR).text,( pdrNode, meta.copy(xmlNode = null)))})

    //2.1 join RDD[ (codPdr,(pdrNode,gasMetadata)) ] with RDD[(codPdr, idPdr)] on codPdr
    //2.2 produce in output tuples of (Option[idPdr],(pdrNode,xmlMetadata)) ready to be joined on idPdr with rcugas_pdr_stato
    //2.3 persist since it will be used twice
    val rddWithPdrExistenceMetadata = rdd.leftOuterJoin(rcuGasPdrRdd)
      .map({case(codPdr,((pdrNode,xmlMeta), idPdr))=> (idPdr,(pdrNode,xmlMeta.copy(pdrRcuExist=idPdr.isDefined)))  })
      .persist(StorageLevel.MEMORY_AND_DISK)

    //2.4 get all pdrNodes whit the codPdr present into rcugas_pdr_p, those will be joined with rcugas_stato_pdr_p to get validity dates for pdr (see 3.1)
    //2.5 prepare data for the next join with key idPdr
    val pdrNodeValidInRcu = rddWithPdrExistenceMetadata.filter(_._1.isDefined)
      .map({case(idPdr,(pdrNode,xmlMeta))=> (idPdr.get,(pdrNode,xmlMeta)) })
    //2.6 set validity dates to impossible values so that pdr won't result valid, ever
    val pdrNodeNotValidInRcu = rddWithPdrExistenceMetadata.filter(_._1.isEmpty)
      .map{case(idPdr,(pdrNode,xmlMeta))=> (pdrNode,xmlMeta.copy(pdrValidFrom=LocalDateTime.MAX,pdrValidTo=LocalDateTime.MAX))}

    //3.1 join (idPdr,(pdrNode,xmlMeta)) with (idPdr, Option[(dataInizio,dataFine)] ) to get valid dates
    //3.2 Map records by adding condition to get only d_inizio and d_fine we are interested in: condition is true only for the range where pdrNode\data_misura falls into
    val pdrValidDates = pdrNodeValidInRcu.leftOuterJoin(rcuGasPdrStatoRdd)
      .map({case(idPdr,((pdrNode,xmlMeta),statoOption))=>{
        val (sDataInizio, sDataFine, stato) = if (statoOption.isDefined) statoOption.get else ("","","")
        val dataInizio = DateTimeUtility.getDateTimeOr(sDataInizio,RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"min")
        val dataFine = DateTimeUtility.getDateTimeOr(sDataFine,RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"max")
        val dataMisuraPdr = Try(LocalDate.parse( (pdrNode\data_misura).text, DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN) ) )

        val condition = (statoOption.isEmpty) ||
          (statoOption.isDefined && dataMisuraPdr.isSuccess && !dataMisuraPdr.get.isAfter(dataFine.toLocalDate) && !dataMisuraPdr.get.isBefore(dataInizio.toLocalDate) )

        (idPdr, ((pdrNode, xmlMeta), statoOption, condition))
      }})
      .persist(StorageLevel.MEMORY_AND_DISK)

    //3.3 Select PdR with invalid ranges (i.e. stato is defined in rcugas_stato_pdr_p table, but no data ranges contains data_misura)
    //3.4 Enrich metadata by inserting invalid pdrValidFrom and pdrValidTo, so that rules based on the values fail
    val pdrWithoutAnyValidRange = pdrValidDates
      .groupByKey
      .filter({ case (idPdr, iterable) => !iterable.exists({ case ((pdrNode, xmlMeta), statoOption, condition) => condition})})
      .map({ case (idPdr, iterable) => (idPdr, iterable.map(_._1).head)})
      .map({ case (idPdr, (pdrNode, xmlMeta)) => (pdrNode, xmlMeta.copy(
        pdrValidFrom = LocalDateTime.MAX,
        pdrValidTo = LocalDateTime.MAX
      ))})

    //3.5 Filter valid ranges, then we enrich metadata using the following reasoning:
    //    - if rcugas_pdr_statp_p.t_cod_stato == P and set metadata valid period range
    //    - otherwise set validity dates to impossible values
    val pdrWithValidRange = pdrValidDates
      .filter({ case (idPdr, ((pdrNode, xmlMeta), statoOption, condition)) => condition})
      .map({ case (idPdr, ((pdrNode, xmlMeta), statoOption, condition)) =>
        if (statoOption.isEmpty){
          (pdrNode,xmlMeta.copy(pdrValidFrom=LocalDateTime.MAX,pdrValidTo=LocalDateTime.MAX))
        }else{
          val (validFrom, validTo, stato) = statoOption.get
          (pdrNode,
            xmlMeta.copy(
              pdrValidFrom = if(stato.equalsIgnoreCase("P")) DateTimeUtility.getDateTimeOr(validFrom,RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"min") else LocalDateTime.MAX,
              pdrValidTo= if(stato.equalsIgnoreCase("P")) DateTimeUtility.getDateTimeOr(validTo,RCUGAS_PDR_STATO_DATE_TIME_PATTERN_OPTIONAL_MS,"max") else LocalDateTime.MAX )
          )
        }
      })

    //4.1 get the union of pdr without a value in rcu and with a value in rcu
    pdrNodeNotValidInRcu
      .union(pdrWithValidRange)
      .union(pdrWithoutAnyValidRange)
      .coalesce(rddWithPdrExistenceMetadata.partitions.length)
      .map({ case (node,meta)=> ( node, meta.copy(xmlNode=XML.loadFile(meta.file)) )})

  }

  def writeValidationReports(xmlWithMessages: RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], unzipTimestamp:String)/*(implicit sc: SparkContext, sqlContext: SQLContext)*/: Unit = {
    val xmlWithMessagesNewLogic = getReportMessagesNewLogic(xmlWithMessages)

    writeAmmissibilitaReportsHive(xmlWithMessagesNewLogic, unzipTimestamp)
    writeAmmissibilitaReportsCsv(xmlWithMessagesNewLogic)
  }
}

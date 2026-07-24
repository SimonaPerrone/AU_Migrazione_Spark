package it.au.misure.aggregazioni

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf

import it.au.misure.util.Schemas._
import java.util.Properties
import it.au.misure.util.CreateProperties
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
import it.au.misure.util.LoggingSupport
//import it.au.misure.cli.CommonsCliUtils.Args

import java.text.SimpleDateFormat
/**
 * ==Flusso Misure Aggregazione Am Misure Master==
 * Acquisisce le misure divise in ore precedentemente elaborate dal processo
 * di aggregazione denominato 'Flusso Misure Aggregazione Misure Orarie' e le aggrega per distributore e codice UC. Al termine dell'aggregazione, il risultato viene salvato
 * nella tabella hdfs ''denominata aggregazioni_misure_am''.
 */
object AmMisureMaster_old extends LoggingSupport{

/**
 * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
 * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
 */
	def main(args: Array[String]) {
	  
	  val commonsCliUtils = new CommonsCliUtils()
	  val commandLineOptions = new CommandLineOptions()
	  val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObj = commonsCliUtils.getArgs(commandLine)

		val propertiesC =new CreateProperties(System.getProperty("user.dir"))
		val prop:Properties = propertiesC.prop

		val _dbDest:String = prop.getProperty("spark.app.dbdest")
		val _basePath:String =prop.getProperty("spark.app.basepath")

		val conf = new SparkConf()
				.setAppName( argsObj.appName )
				.set("spark.shuffle.service.enabled", "false")
				.set("spark.dynamicAllocation.enabled", "false")
				.set("spark.io.compression.codec", "snappy")
				.set("spark.rdd.compress", "true")
				.setMaster( argsObj.master )

				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")
				
				val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

				val sqlCtx = new SQLContext(sc)
				sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
				sqlCtx.setConf("spark.sql.parquet.compression.codec","uncompressed")
				sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
				sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
//				sqlCtx.setConf("spark.sql.shuffle.partitions", minPartitions)
//			  sqlCtx.setConf("spark.default.parallelism", minPartitions)


				val orarie =  sc.getConf.get("spark.aggregazioni.misure.orarie")
				val am =  sc.getConf.get("spark.aggregazioni.misure.am")
//				val anno:String = argsObj.anno
//				val mese:String = argsObj.mese
			  val annoAggr:String =  argsObj.anno
				val meseAggr:String =  argsObj.mese
//				val ghigliottina:Int = argsObj.annomesegiornodir
				val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
	  	  val uidElab = sdf.format(new java.util.Date()).toLong
				
				log.info(s"***** Inizio processo ${argsObj.appName} *****")
				log.info(propertiesC.printEnvVar)
		    log.info("***** current user " + System.getProperty("user.name") + "****")

				log.info("*** sc.master: " + sc.master)
			  log.info("*** orarie: " + orarie)
	      log.info("*** am: " + am)
	      log.info("*** minPartitions: " + minPartitions)
//			  log.info("*** anno: " + anno)
//			  log.info("*** mese: " + mese)
			  log.info("*** annoAggr: " + annoAggr)
			  log.info("*** meseAggr: " + meseAggr)
//			  log.info("*** ghigliottina: " + ghigliottina)
				log.info("*** orarie: " + orarie)

				val db = _dbDest

				log.info("*** database di destinazione : "+ db)
				
					val query:String = if(commandLine.hasOption(commandLineOptions.distrAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.distrAgg.getOpt).split(',').toList
					  s"and n_id_distr in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else  if(commandLine.hasOption(commandLineOptions.uteAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.uteAgg.getOpt).split(',').toList
					  s"and n_id_udd in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else if(commandLine.hasOption(commandLineOptions.noDistrAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.noDistrAgg.getOpt).split(',').toList
					  s"and n_id_distr not in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else  if(commandLine.hasOption(commandLineOptions.noUteAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.noUteAgg.getOpt).split(',').toList
					  s"and n_id_udd not in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else {
					  ""
					}
				

	  		 val whereCond = s"annoaggr=${annoAggr.toInt} and meseaggr=${meseAggr.toInt} ${ query } "



					val dfAggr3 = if(true){//if (!commandLine.hasOption(commandLineOptions.local.getOpt)) {
							val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
							val view =	hiveContext.sql(s"select T.*,${uidElab} versione from au.aggreagati_am_view T")
								.where( whereCond )
								view
						}else{
						  val view = sqlCtx.read.parquet("hdfs://nm.dominio.local/user/ec2-user/au/misure_ee_au/aggreagati_am_view")
						    .where( whereCond )
						    view
						}
						log.info("***** select aggreagati_am_view OK")
						
						
			val refill:String => String = ( f => ("0".concat(f.toString()) takeRight 2) )
      val refillUDF = udf(refill)
      
      val refillDistr:String => String = ( f => ("000000".concat(f)  takeRight 11) )
      val refillDistrUDF = udf(refillDistr)
      
      val rounding:Double => Double = ( BigDecimal( _ ).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble )
      val roundingUDF = udf(rounding)
						

		val dfAggr4 = dfAggr3
      .select(
          col("n_id_udd").cast(StringType),
          col("n_id_distr").cast(StringType),
          col("n_id_distr_rif").cast(StringType),
          col("area"),
          col("annoaggr"),
          col("meseaggr"),
          refillDistrUDF(col("pivadistributoreaggr")).alias("pivadistributoreaggr"),
          col("giornoaggr"),
          col("dataelaborazione"),
          col("versione"),
          col("versione_orarie"),
          roundingUDF(col("h1")).alias("h1"),
          roundingUDF(col("h2")).alias("h2"),
          roundingUDF(col("h3")).alias("h3"),
          roundingUDF(col("h4")).alias("h4"),
          roundingUDF(col("h5")).alias("h5"),
          roundingUDF(col("h6")).alias("h6"),
          roundingUDF(col("h7")).alias("h7"),
          roundingUDF(col("h8")).alias("h8"),
          roundingUDF(col("h9")).alias("h9"),
          roundingUDF(col("h10")).alias("h10"),
          roundingUDF(col("h11")).alias("h11"),
          roundingUDF(col("h12")).alias("h12"),
          roundingUDF(col("h13")).alias("h13"),
          roundingUDF(col("h14")).alias("h14"),
          roundingUDF(col("h15")).alias("h15"),
          roundingUDF(col("h16")).alias("h16"),
          roundingUDF(col("h17")).alias("h17"),
          roundingUDF(col("h18")).alias("h18"),
          roundingUDF(col("h19")).alias("h19"),
          roundingUDF(col("h20")).alias("h20"),
          roundingUDF(col("h21")).alias("h21"),
          roundingUDF(col("h22")).alias("h22"),
          roundingUDF(col("h23")).alias("h23"),
          roundingUDF(col("h24")).alias("h24"),
          roundingUDF(col("h25")).alias("h25")
        )


				dfAggr4
				.write
				.format("parquet")
				.mode(SaveMode.Append)
				.partitionBy("annoaggr","meseaggr","pivadistributoreaggr","versione")
				.save(am);
		    log.info("***** insert aggregazioni_misure_am su hdfs OK")
		    
		    
		          
     /*
			* aggiorno le partizioni
			*/
		    if (true) {//(!commandLine.hasOption(commandLineOptions.local.getOpt)) {
		    	val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
		    			hiveContext.sql("MSCK REPAIR TABLE " + db +".aggregazioni_misure_am")
		    			log.info("***** aggiornamento partizioni OK")
		    }
		    
		    
		  log.info("***** insert aggregazioni_misure_am su rdbms ")

			val jdbcUrl:String = prop.getProperty("spark.app.url")
			val jdbcUsername:String = prop.getProperty("spark.app.user")
			val jdbcPassword:String = prop.getProperty("spark.app.password")
		  val driver = prop.getProperty("spark.app.jdbc.driver")
			Class.forName(driver)
			
			log.info(s"*** jdbcUrl: ${jdbcUrl}")
			log.info(s"*** jdbcUsername: ${jdbcUsername}")
			log.info(s"*** jdbcPassword: ${jdbcPassword}")
			log.info(s"*** driver: ${driver}")
			
			val connectionProperties = new Properties()
		  connectionProperties.put("user", jdbcUsername)
      connectionProperties.put("password", jdbcPassword)
      connectionProperties.setProperty("Driver", driver)
      
      val toint:Int => Int = ( _.toInt )
      val toIntUDF = udf(toint)
      
      val aggrCalc = dfAggr3
      .select(
          col("n_id_distr").cast(StringType).alias("N_ID_DISTR"),
          col("area").alias("T_AREA_RIF"),
          toIntUDF(concat(col("annoaggr"),
          refillUDF(col("meseaggr")))).alias("ANNOMESE"),
          col("n_id_udd").cast(StringType).alias("N_ID_UDD"),
          col("giornoaggr").alias("GIORNO"),
          roundingUDF(col("h1")).alias("N_H1"),
          roundingUDF(col("h2")).alias("N_H2"),
          roundingUDF(col("h3")).alias("N_H3"),
          roundingUDF(col("h4")).alias("N_H4"),
          roundingUDF(col("h5")).alias("N_H5"),
          roundingUDF(col("h6")).alias("N_H6"),
          roundingUDF(col("h7")).alias("N_H7"),
          roundingUDF(col("h8")).alias("N_H8"),
          roundingUDF(col("h9")).alias("N_H9"),
          roundingUDF(col("h10")).alias("N_H10"),
          roundingUDF(col("h11")).alias("N_H11"),
          roundingUDF(col("h12")).alias("N_H12"),
          roundingUDF(col("h13")).alias("N_H13"),
          roundingUDF(col("h14")).alias("N_H14"),
          roundingUDF(col("h15")).alias("N_H15"),
          roundingUDF(col("h16")).alias("N_H16"),
          roundingUDF(col("h17")).alias("N_H17"),
          roundingUDF(col("h18")).alias("N_H18"),
          roundingUDF(col("h19")).alias("N_H19"),
          roundingUDF(col("h20")).alias("N_H20"),
          roundingUDF(col("h21")).alias("N_H21"),
          roundingUDF(col("h22")).alias("N_H22"),
          roundingUDF(col("h23")).alias("N_H23"),
          roundingUDF(col("h24")).alias("N_H24"),
          roundingUDF(col("h25")).alias("N_H25"),
          col("dataelaborazione").alias("D_DATA_AGGREGAZIONE"),
          lit("N").alias("T_AGGR_SOTTESI"),//default 'N'
          col("n_id_distr_rif").cast(StringType).alias("N_ID_DISTR_RIF"),
          col("versione").alias("UID_ELAB"),
          col("versione_orarie").alias("UID_ELAB_ORARIE")
        )
        
        
//        lit(uidElab).alias("UID_ELAB"),

    	  aggrCalc
    	  .write
    	  .mode(SaveMode.Append)
    	  .jdbc(jdbcUrl, "PRT_TMO_AGGREGATI_CALCOLATI", connectionProperties)
    	  
    	  log.info("*** write su PRT_TMO_AGGREGATI_CALCOLATI OK")
    	  log.info(s"SELECT * FROM PRT_TMO_AGGREGATI_CALCOLATI WHERE ANNOMESE=${annoAggr}${meseAggr} AND UID_ELAB=${uidElab};")

      
		sc.stop()

    log.info(s"*** Fine processo ${argsObj.appName} *****")
	}
	

  def bigDecimalFormatter(x: Double) = BigDecimal(x).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble

}
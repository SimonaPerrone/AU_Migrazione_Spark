package it.au.misure.aggregazioni

import java.util.Properties

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
//import it.au.misure.cli.CommonsCliUtils.Args

import java.text.SimpleDateFormat

/**
 * ==Flusso Misure Aggregazione Am Misure Master==
 * Acquisisce le misure divise in ore precedentemente elaborate dal processo
 * di aggregazione denominato 'Flusso Misure Aggregazione Misure Orarie' e le aggrega per distributore e codice UC. Al termine dell'aggregazione, il risultato viene salvato
 * nella tabella hdfs ''denominata aggregazioni_misure_am''.
 */
object AmMisureMaster extends LoggingSupport{

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
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.setMaster( argsObj.master )

				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")
				sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
				sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

				val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

				val hiveCtx = new HiveContext(sc)
				hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
				hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
				hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
				hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
				hiveCtx.setConf("hive.exec.dynamic.partition", "true")
				hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

				hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
				hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
				hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")


				val orarie =  sc.getConf.get("spark.aggregazioni.misure.orarie")
				val am =  sc.getConf.get("spark.aggregazioni.misure.am")

			  val annoAggr:String =  argsObj.anno
				val meseAggr:String =  argsObj.mese

				val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
	  	  val uidElab = sdf.format(new java.util.Date()).toLong
				
				log.info(s"***** Inizio processo ${argsObj.appName} *****")
				log.info(propertiesC.printEnvVar)
		    log.info("***** current user " + System.getProperty("user.name") + "****")

				log.info("*** sc.master: " + sc.master)

		   val uid_to_use=if(commandLine.hasOption(commandLineOptions.uid_aggr_triple.getOpt))
			   commandLine.getOptionValue(commandLineOptions.uid_aggr_triple.getOpt)
	     	else ""

		   if(uid_to_use!="")
		  	log.info("*** Scrittura diretta su Oracle - orarie da utilizzare  " + uid_to_use )
       else
		    log.info("*** orarie: " + orarie)

	      log.info("*** am: " + am)
		     val y=am.split("/")
		     val tbl_am= y(y.length-1)

	      log.info("*** minPartitions: " + minPartitions)

			  log.info("*** annoAggr: " + annoAggr)
			  log.info("*** meseAggr: " + meseAggr)

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




		val refill: String => String = (f => ("0".concat(f.toString()) takeRight 2))
		val refillUDF = udf(refill)

		val refillDistr: String => String = (f => ("000000".concat(f) takeRight 11))
		val refillDistrUDF = udf(refillDistr)

		val rounding: Double => Double = (BigDecimal(_).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble)
		val roundingUDF = udf(rounding)

		if(uid_to_use=="") {
			val whereCond = s"annoaggr=${annoAggr.toInt} and meseaggr=${meseAggr.toInt} ${query} "

			/* val dfAggr3 = hiveCtx.sql(s"""
		     select T.*,${uidElab} versione
		     from ${db}.aggreagati_am_view T
		     """).where( whereCond )*/

			val dfAggr3 = hiveCtx.sql(
				s"""
		     select n_id_udd,n_id_distr,n_id_distr_rif,area,annoaggr,meseaggr,
		     substr(concat('000000',pivadistributoreaggr),-11) pivadistributoreaggr,
         giornoaggr,dataelaborazione,${uidElab} versione,versione_orarie,
				 round(h1,0)h1,round(h2,0)h2,round(h3,0)h3,round(h4,0)h4,round(h5,0)h5,
				 round(h6,0)h6,round(h7,0)h7,round(h8,0)h8,round(h9,0)h9,round(h10,0)h10,
				 round(h11,0)h11,round(h12,0)h12,round(h13,0)h13,round(h14,0)h14,
				 round(h15,0)h15,round(h16,0)h16,round(h17,0)h17,round(h18,0)h18,
				 round(h19,0)h19,round(h20,0)h20,round(h21,0)h21,round(h22,0)h22,
				 round(h23,0)h23,round(h24,0)h24,round(h25,0)h25
				 from ${db}.aggreagati_am_view T where ${whereCond}
		     """)
			//.where( whereCond )

			log.info("***** select " + db + ".aggreagati_am_view OK")

			/*
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
		*/

			val dfAggr4 = dfAggr3
				.select(
					col("n_id_udd").cast(StringType),
					col("n_id_distr").cast(StringType),
					col("n_id_distr_rif").cast(StringType),
					col("area"),
					col("annoaggr"),
					col("meseaggr"),
					col("pivadistributoreaggr"),
					col("giornoaggr"),
					col("dataelaborazione"),
					col("versione"),
					col("versione_orarie"),
					col("h1"),
					col("h2"),
					col("h3"),
					col("h4"),
					col("h5"),
					col("h6"),
					col("h7"),
					col("h8"),
					col("h9"),
					col("h10"),
					col("h11"),
					col("h12"),
					col("h13"),
					col("h14"),
					col("h15"),
					col("h16"),
					col("h17"),
					col("h18"),
					col("h19"),
					col("h20"),
					col("h21"),
					col("h22"),
					col("h23"),
					col("h24"),
					col("h25")
				)

			log.info("Scrittura in " + am)
			dfAggr4
				.write
				.format("parquet")
				.mode(SaveMode.Append)
				.partitionBy("annoaggr", "meseaggr", "pivadistributoreaggr", "versione")
				.save(am);
			log.info("***** insert aggregazioni_misure_am su hdfs OK")


			/*
			* aggiorno le partizioni
			*/

			hiveCtx.sql("MSCK REPAIR TABLE " + db + ".aggregazioni_misure_am")
			log.info("***** aggiornamento partizioni OK")

		}
/*
  n_id_udd String,n_id_distr String, n_id_distr_rif String, area String, giornoaggr INT, dataelaborazione TIMESTAMP,versione_orarie BIGINT,
  h1 DOUBLE,h2 DOUBLE,h3 DOUBLE,h4 DOUBLE,h5 DOUBLE,h6 DOUBLE,h7 DOUBLE,h8 DOUBLE,h9 DOUBLE,h10 DOUBLE,h11 DOUBLE,h12 DOUBLE,h13 DOUBLE,h14 DOUBLE,h15 DOUBLE,h16 DOUBLE,h17 DOUBLE,h18 DOUBLE,h19 DOUBLE,h20 DOUBLE,h21 DOUBLE,h22 DOUBLE,h23 DOUBLE,h24 DOUBLE,h25 DOUBLE
  annoaggr INT,meseaggr INT,pivadistributoreaggr String,versione BIGINT
 */
		 val orarieToUse = if(uid_to_use=="")uidElab.toString else uid_to_use
		 val queryTerne="select * from " + db + s".${tbl_am} where annoaggr = " + annoAggr.toString + " and meseAggr = " + meseAggr.toString + " and versione = " + orarieToUse
		 val dtTerne =hiveCtx.sql(queryTerne)

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


      //val aggrCalc = dfAggr3
			val aggrCalc = dtTerne
      .select(
          col("n_id_distr").cast(StringType).alias("N_ID_DISTR"),
          col("area").alias("T_AREA_RIF"),
          toIntUDF(concat(col("annoaggr"),
          refillUDF(col("meseaggr")))).alias("ANNOMESE"),
          col("n_id_udd").cast(StringType).alias("N_ID_UDD"),
          col("giornoaggr").alias("GIORNO"),
          col("h1").alias("N_H1"),
          col("h2").alias("N_H2"),
          col("h3").alias("N_H3"),
          col("h4").alias("N_H4"),
          col("h5").alias("N_H5"),
          col("h6").alias("N_H6"),
          col("h7").alias("N_H7"),
          col("h8").alias("N_H8"),
          col("h9").alias("N_H9"),
          col("h10").alias("N_H10"),
          col("h11").alias("N_H11"),
          col("h12").alias("N_H12"),
          col("h13").alias("N_H13"),
          col("h14").alias("N_H14"),
          col("h15").alias("N_H15"),
          col("h16").alias("N_H16"),
          col("h17").alias("N_H17"),
          col("h18").alias("N_H18"),
          col("h19").alias("N_H19"),
          col("h20").alias("N_H20"),
          col("h21").alias("N_H21"),
          col("h22").alias("N_H22"),
          col("h23").alias("N_H23"),
          col("h24").alias("N_H24"),
          col("h25").alias("N_H25"),
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
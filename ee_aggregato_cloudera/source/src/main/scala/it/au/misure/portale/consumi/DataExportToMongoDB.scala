package it.au.misure.portale.consumi

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties, TimeZone}

import it.au.misure.util.{CreateProperties, LoggingSupport}
import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import java.util.UUID.randomUUID

import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import com.mongodb.spark._
import com.mongodb.spark.config.{MongoCollectionConfig, ReadConfig, WriteConfig}
import org.bson._
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.MongoNamespace
import com.mongodb.DBCollection
import com.mongodb.DBObject
import org.bson
import org.mongodb.scala.MongoCollection

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object DataExportToMongoDB extends LoggingSupport {

  val format = new SimpleDateFormat("yyyy-MM-dd")
  val propertiesC =new CreateProperties(System.getProperty("user.dir"))
  val prop:Properties = propertiesC.prop
  var hiveCtx:HiveContext=null



  val connectionStringMongo= prop.getProperty("spark.app.mongodb.connstr.produzione") // "mongodb://pdc:AueMongo123@172.16.16.71:27017,172.16.16.72:27107,172.16.16.73:27107"
  val connectionStringMongoCollaudo=prop.getProperty("spark.app.mongodb.connstr.collaudo")//"mongodb://pdc:AueMongo123@172.16.16.70:27017"
  val writeConcern_Write_Prod= if(prop.getProperty("spark.app.mongodb.writeconcern_write.produzione")=="") "majority" else prop.getProperty("spark.app.mongodb.writeconcern_write.produzione")
  val writeConcern_Journal_Prod= if(prop.getProperty("spark.app.mongodb.writeconcern_journal.produzione")=="") "true" else prop.getProperty("spark.app.mongodb.writeconcern_journal.produzione")
  //val writeConcern_Timeout_Prod= if(prop.getProperty("spark.app.mongodb.writeconcern_timeout.produzione")=="") "0" else prop.getProperty("spark.app.mongodb.writeconcern_timeout.produzione")

  var dbgas= prop.getProperty("spark.app.mongodb.dbgas.input")

  var mongouri:String=""

  /*val elab_34mesi:Boolean=propertiesC.PortaleConsumi34Mesi
  val elab_2mesi:Boolean=propertiesC.PortaleConsumi2Mesi
  
  val suffix_m=if(elab_34mesi)"_34" else if(elab_2mesi) "_2" else ""
  val suffix_forn=if(elab_34mesi || elab_2mesi)"_36" else ""

  var mongocollaudo:Boolean=if(elab_34mesi || elab_2mesi)true else prop.getProperty("spark.app.mongodb.usecollaudo").toBoolean//true
  var suffixCollection= if(elab_34mesi || elab_2mesi)suffix_m else prop.getProperty("spark.app.mongodb.suffix.collection")
  var suffixCollection_Forn= if(elab_34mesi || elab_2mesi)suffix_forn else prop.getProperty("spark.app.mongodb.suffix.collection")
*/
  val suffix_m= "" //scrittura tabelle misure su una di test con finisce con il suffisso specificato
  val suffix_forn= "" //scrittura tabelle forniture su una di test con finisce con il suffisso specificato

  var mongocollaudo:Boolean= prop.getProperty("spark.app.mongodb.usecollaudo").toBoolean//true
  var suffixCollection=  prop.getProperty("spark.app.mongodb.suffix.collection")
  var suffixCollection_Forn=  prop.getProperty("spark.app.mongodb.suffix.collection")


  var force_collaudo:Boolean=false

  var process_splitted_mis=false
  var process_last_3_month=false
  //172.16.16.70 = collaudo

  //date in formato yyyymmdd

  def main(args: Array[String]) {

    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgsPort_Consumi(commandLine)


    val createindexColl: Boolean = if (argsObjMaster.PdoRfo.contains("-I") || argsObjMaster.PdoRfo == "I") true else false
    force_collaudo  = argsObjMaster.PdoRfo.contains("-C")

    if(force_collaudo){
      mongocollaudo=true
      //dbgas="misuregas"

    }

    //il comando si aspetta o SPLIT3 o SPLIT33
    process_splitted_mis=argsObjMaster.PdoRfo.contains("-SPLIT3") || argsObjMaster.PdoRfo.contains("-SPLIT33")
    process_last_3_month=if(argsObjMaster.PdoRfo.contains("-SPLIT33")) false else argsObjMaster.PdoRfo.contains("-SPLIT3")

    val tipoExport = argsObjMaster.PdoRfo.replace("-I", "").replace("-C","").replace("-SPLIT33","").replace("-SPLIT3","")
    if(argsObjMaster.PdoRfo.contains("-I") && mongocollaudo)
    {
     log.info("In ambiente di collaudo non è possibile eseguire la rinomina e la creazione degli indici")
     return
    }

    var descrTipoExp = if (tipoExport == "F") "FORNITURE EE" else if (tipoExport == "M" || tipoExport == "T") "MISURE EE" else if (tipoExport == "FG") "FORNITURE GAS" else if (tipoExport == "MG") "MISURE GAS" else if (tipoExport == "I") "CREAZIONE INDICI" else ""


    if (descrTipoExp == "") {
      log.info("Bisogna indicare una opzione tra M[-I[-SPLIT3|-SPLIT33]]/F[-I]/FG[-I]/MG[-I[-SPLIT3|-SPLIT33]]/I (MISURE ELETTRICHE/FORNITURE ELETTRICHE/FORNITURE GAS/MISURE GAS/CREAZIONE INDICI )")
      return
    }

    log.info(s"**** Tipo di export : ${tipoExport} ****")

    val nameApp = argsObjMaster.appName + " - " + descrTipoExp
    log.info("***** Inizio processo " + nameApp + " *****")

    log.info("***** current user " + System.getProperty("user.name") + "****")
    log.info(propertiesC.printEnvVar)
    val cur_user = System.getProperty("user.name")

    log.info(s"**** Forzatura a collaudo : ${force_collaudo} ****")
    log.info(s"**** Database per forniture e misure gas : ${dbgas} ****")
    log.info("**** Utilizzo mongodb di collaudo :" + mongocollaudo.toString + " ****")

    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));

    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    val d_max = anno + mese + giorno
    val d_min = (anno.toInt - 1).toString + mese + giorno
    val d_min_mese = (anno.toInt - 1).toString + mese


    val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val uidElab = sdf.format(new java.util.Date()).toLong

    if(mongocollaudo)
      {
        suffixCollection=s"_${uidElab}"
        suffixCollection_Forn=s"_${uidElab}"
      }
    val conf = new SparkConf()
      .setAppName(nameApp)

      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster(argsObjMaster.master)


    var dbMongo = ""
    var collectionMongo = ""
    var collectionMongoDescr = ""
    var suffix_index=""

    if (tipoExport == "F") {
      dbMongo = "Forniture"
      collectionMongo = "FornitureElettriche" + (suffixCollection_Forn)
      collectionMongoDescr=collectionMongo
    }
    else if (tipoExport == "M") {
      dbMongo = "Forniture"
      val suffix= if(process_splitted_mis)
      { if(process_last_3_month)"3M" else "33M" }else ""

      collectionMongo = "MisureElettriche" + suffix + suffixCollection
      collectionMongoDescr=collectionMongo
    }
    else if (tipoExport == "FG") {
      dbMongo = "Forniture"
      collectionMongo = "FornitureGas" + (suffixCollection_Forn)
      collectionMongoDescr=collectionMongo
    }
    else if (tipoExport == "MG") {
      dbMongo = "Forniture"
      val suffix= if(process_splitted_mis)
      { if(process_last_3_month)"3M" else "33M" }else ""
      collectionMongo = "MisureGas" + suffix + suffixCollection
      collectionMongoDescr=collectionMongo
    }
    else if (tipoExport == "I") {
      dbMongo = "Forniture"
      collectionMongo = "Indici"
      suffix_index =if(process_splitted_mis)
      { if(process_last_3_month)"3M" else "33M" }else ""

      collectionMongoDescr = "FornitureElettriche" + (suffixCollection_Forn)  + " , " + "FornitureGas" + (suffixCollection_Forn)


    }


    mongouri = if (mongocollaudo) s"${connectionStringMongoCollaudo}/${dbMongo}.${collectionMongo}" else s"${connectionStringMongo}/${dbMongo}.${collectionMongo}?replicaSet=rset&authSource=${dbMongo}"


    conf.set("spark.mongodb.input.uri", s"${mongouri}")
    conf.set("spark.mongodb.output.uri", s"${mongouri}")


    log.info(mongouri)

    //if(!mongocollaudo) {
      log.info(s"WriteConcern Write : ${writeConcern_Write_Prod}")
      log.info(s"WriteConcern Journal : ${writeConcern_Journal_Prod}")
     // log.info(s"WriteConcern Timeout Milliseconds(0 = no timeout) : ${writeConcern_Timeout_Prod}")
    //}

    log.info("database :" + dbMongo + " , collection : " + collectionMongoDescr)


    val sc: SparkContext = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


    sc.setLogLevel(argsObjMaster.logLevel)

    log.info("*** sc.master: " + sc.master)

    val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions")


    hiveCtx = new HiveContext(sc)
    hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
    hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
    hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    hiveCtx.setConf("hive.exec.dynamic.partition", "true")
    hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
    hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
    hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")

    log.info("Test di connessione a mongo db")
    val df = sc.loadFromMongoDB()
    log.info("Connessione a mongo db OK")


    try {
      if (tipoExport == "F") {
        ExportFornitureElettrico(dbMongo, collectionMongo)
        ExportFornitureElettrico2(dbMongo, collectionMongo)
        if (createindexColl || force_collaudo) CreateIndex(dbMongo, collectionMongo, "codice_fiscale")
        if(mongocollaudo)saveLastCollectionWrittedtoHive(dbMongo,uidElab.toString,"FornitureElettriche",collectionMongo)
      }
      else if (tipoExport == "M") {
        ExportMisureElettrico(dbMongo, collectionMongo)
        if (createindexColl || force_collaudo) CreateIndex(dbMongo, collectionMongo, "_id")
        val tmp =if(process_last_3_month)"3M" else "33M"
        if(mongocollaudo)saveLastCollectionWrittedtoHive(dbMongo,uidElab.toString,s"MisureElettriche${tmp}",collectionMongo)
      }
      else if (tipoExport == "FG") {
        ExportFornitureGas(dbMongo, collectionMongo)
        ExportFornitureGas2(dbMongo, collectionMongo)
        if (createindexColl || force_collaudo) CreateIndex(dbMongo, collectionMongo, "codice_fiscale")
        if(mongocollaudo)saveLastCollectionWrittedtoHive(dbMongo,uidElab.toString,"FornitureGas",collectionMongo)
      }
      else if (tipoExport == "MG") {
        ExportMisureGas(dbMongo, collectionMongo)
        if (createindexColl || force_collaudo) CreateIndex(dbMongo, collectionMongo, "_id")
        val tmp =if(process_last_3_month)"3M" else "33M"
        if(mongocollaudo)saveLastCollectionWrittedtoHive(dbMongo,uidElab.toString,s"MisureGas${tmp}",collectionMongo)
      }
      else if (tipoExport == "I") {

        CreateIndex(dbMongo, "FornitureElettriche" + (suffixCollection_Forn), "codice_fiscale")
        CreateIndex(dbMongo, "FornitureGas" + (suffixCollection_Forn), "codice_fiscale")
       // CreateIndex(dbMongo, "MisureElettriche" + suffix_index + (suffixCollection), "_id")
       // CreateIndex(dbMongo, "MisureGas" + suffix_index + (suffixCollection), "_id")
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sc.stop
      log.info(s"***** Fine processo ${nameApp} *****")
    }
  }

  /*
  ESEMPIO INSERIMENTO NUOVO DATO GIORNALIERO NELLE MISURE ELETTRICHE
  val mongoClient: MongoClient = new MongoClient(new MongoClientURI("mongodb://pdc:AueMongo123@172.16.16.71:27017,172.16.16.72:27107,172.16.16.73:27107/Forniture"))
val mdb = mongoClient.getDatabase("Forniture");


mdb.getCollection("MisureElettriche_old").updateOne(new Document("_id", "140317000051632347"),
new Document("$addToSet", (new Document("misure.misure_orarie",
new Document("competenza_consumi","202005").
append("consumo_giornaliero", "0.0").
append("lettura_misura_f1", "1901.24").
append("lettura_misura_f2", "1127.24").
append("lettura_misura_f3", "1393.24").
append("lettura_misura_f4", "0.0").
append("lettura_misura_f5", "0.0").
append("lettura_misura_f6", "0.0").
append("delta_misure_f1", "1").
append("delta_misure_f2", "14").
append("delta_misure_f3", "6").
append("delta_misure_f4", "0").
append("delta_misure_f5", "0").
append("delta_misure_f6", "0").
append("giorno", "20200515").
append("potenza_max_erogata", "0.50").
append("tipo_misura", "Lettura Periodica").
append("data_lettura", "20200515")
))
));

   */

  def saveLastCollectionWrittedtoHive(dbMongo:String,uid:String,collectionbase:String,collectiondetail:String): Unit =
  {
    println(s"Salvataggio su hive collection mongodb appena scritta : ${collectionbase} = ${collectiondetail}")
    hiveCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveCtx.sql("set hive.exec.dynamic.partition=true")

    val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
    val mongoClient: MongoClient = new MongoClient(new MongoClientURI(connString))
    //comandi per creare la collection in shard
    val dbx = mongoClient.getDatabase(dbMongo);
    val collaudo_prod=if(mongocollaudo)"C" else "P"

    val vals = hiveCtx.sql(s"select timeid,collectiondetail from misure.collection_writted where collaudo_produzione='${collaudo_prod}' and collectionbase='${collectionbase}'").collect()

    val qins=s"""insert into misure.collection_writted partition(collaudo_produzione,timeid,collectiondetail)
                  select collectionbase,collaudo_produzione,timeid,collectiondetail from (
                   select '${collaudo_prod}' collaudo_produzione , '${collectionbase}' collectionbase,'${collectiondetail}' collectiondetail,'${uid}' timeid
                   ) as tbl
             """
    hiveCtx.sql(qins);

    for(r <- vals)
      {
        val timeid=r(0).toString()
        val collname=r(1).toString()

        val coll_old = dbx.getCollection(collname)
        var ok =DropCollectionMongo(collaudo_prod,coll_old,timeid,collname)
        var numtry=1
        while(!ok || numtry > 10)
          {
            numtry=numtry+1
            Thread.sleep(30000)
            ok =DropCollectionMongo(collaudo_prod,coll_old,timeid,collname)
          }
      }


  }

  def DropCollectionMongo(collaudo_prod:String,mongoColl:com.mongodb.client.MongoCollection[Document],timeid:String,collname:String): Boolean =
  {
    try
    {
      println(s"Tentativo di drop della collection mongodb ${collname}")
      if(mongoColl.count() > 0) {
        mongoColl.drop()
        mongoColl.drop()
      }
      hiveCtx.sql(s"ALTER TABLE misure.collection_writted DROP IF EXISTS PARTITION (collaudo_produzione='${collaudo_prod}',timeid='${timeid}',collectiondetail='${collname}')")
      true
    }
    catch {
      case e: Exception => {
        println(s"Errore nella drop della collection mongodb ${collname} causa : ${e.getMessage}")
        false}
    }
  }
  def CreateIndex(dbMongo :String,collectionMongo:String,fieldIndex:String): Unit = {
    try
      {

          val connString = if (mongocollaudo) connectionStringMongoCollaudo + "/Forniture" else connectionStringMongo + "/Forniture"
          val mongoClient: MongoClient = new MongoClient(new MongoClientURI(connString))
          val db = mongoClient.getDatabase(dbMongo);

          if(fieldIndex!="_id") {

            log.info(s"Avvio creazione indice ${fieldIndex} per la collection : " + collectionMongo)


            val collection = db.getCollection(collectionMongo);

          if (collection.count() == 0) {

            log.info(s"Non è stato possibile trovare la collection : ${collectionMongo}")
            return
          }
          //val indexOptions = new IndexOptions().background(true).name(fieldIndex)
          //val res = collection.createIndex(new BasicDBObject(fieldIndex, 1), indexOptions)

            val res = collection.createIndex(new BasicDBObject(fieldIndex, 1))

          log.info("Indice creato :" + res)
         }else
          {
            log.info(s"L'indice _id per la collection ${collectionMongo} è automaticamente generato da mongodb")
          }

          //in collaudo essendo in sharded non è possibile eseguire la rinomina
        if(mongocollaudo)
          {
            //ricavare le collection da una tabella hive e ottenere tutte le collections
            //piu vecchie da cancellare

            return
          }

        if(!mongocollaudo) {

          val collectionA = collectionMongo.replace(suffixCollection, "")
          if (db.getCollection(collectionMongo).count() == 0) {
            log.info(s"La collection : ${collectionMongo} non esiste. Rinomina annullata!")
            return;
          }
          try {
            if(db.getCollection(s"${collectionA}_old").count()>0) {
              db.getCollection(s"${collectionA}_old").drop()
              log.info(s"Cancellazione della collection ${collectionA}_old ")
            }
          } catch {
            case e: Exception => {}
          }

          try {

            if (db.getCollection(collectionMongo).count() == 0) {
              log.info(s"La collection : ${collectionMongo} non esiste. Rinomina annullata!")
              return
            }

            val newName = new MongoNamespace(dbMongo, s"${collectionA}_old");
            if(db.getCollection(collectionA).count()>0) {
              db.getCollection(collectionA).renameCollection(newName)
              log.info(s"Rinominata collection ${collectionA} in ${collectionA}_old")
            }

            val newName2 = new MongoNamespace(dbMongo, collectionA);
            db.getCollection(collectionMongo).renameCollection(newName2)
            log.info(s"Rinominata collection ${collectionMongo} in ${collectionA}")

            db.getCollection(s"${collectionA}_old").drop()
            log.info(s"Cancellazione della collection ${collectionA}_old ")

          } catch {
            case e: Exception => {
              log.info("errore causa:" + e.getMessage)
            }
          }

        }

      }catch {
      case e: Exception => {
        log.info("Errore in fase di creazione dell'indice causa :" + e.getMessage)
      }
    }



  }
  def callbackWhenFinished(): Unit ={
    println("finished")
  }


  def getShardDistribution(collection: DBCollection): Int = {
    val stats = collection.getStats
    // Verifico se la  collection è sharded
    if (stats.get("sharded") != null && stats.get("sharded").asInstanceOf[Boolean]) {
      val config = collection.getDB.getSisterDB("config")
      var diffCountChunks:Long = 0
      val shards = stats.get("shards").asInstanceOf[BasicDBObject]

      for (key <- shards.keySet) {
        val shardDoc = config.getCollection("shards").findOne(new BasicDBObject("_id", key))
        println("Shard " + key + " a " + (if (shardDoc != null) shardDoc.get("host")
        else " shardDoc non trovato"))
        val shardStats = shards.get(key).asInstanceOf[DBObject]
        val count_chuncks =config.getCollection("chunks").count(new BasicDBObject().append("shard",key).append("ns",collection.getDB.getName + "." + collection.getName))
        println(s"Collection: ${collection.getDB.getName}.${collection.getName} , Shard :${key} , count chuncks : ${count_chuncks}")

        diffCountChunks =if(diffCountChunks==0)count_chuncks else diffCountChunks -  count_chuncks
      }

      diffCountChunks=Math.abs(diffCountChunks)
      println(s"Differenza di count shard : ${diffCountChunks}")

      diffCountChunks.toInt

    }
    else {
      println("Collection " + collection.getName + " non è sharded.")
      println("Maggiori info: " + stats)
      -1
    }
  }




   def ExportFornitureElettrico(dbMongo :String,collectionMongo:String): Unit = {

     if (mongocollaudo) {

       log.info(s"Impostazione collection ${dbMongo}.${collectionMongo} in modalità shard")
       val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
       val mongoClient = new MongoClient(new MongoClientURI(connString))
       val db = mongoClient.getDatabase("admin");


       //comandi per creare la collection in shard
       val dbx = mongoClient.getDatabase(dbMongo);
      /* val coll_old = dbx.getCollection(collectionMongo);
       coll_old.drop()
       coll_old.drop()
       Thread.sleep(5000)*/

       dbx.createCollection(collectionMongo)

       val documentA = db.runCommand(new Document("shardcollection", s"${dbMongo}.${collectionMongo}").append("key", new Document("codice_fiscale", 1)))

       log.info(s"Esecuzione split sulla collection ${dbMongo}.${collectionMongo}")

       val vals = hiveCtx.sql(s"select distinct chiave from misure.elenco_splits where collection='FornitureElettriche'").collect()

       for(r <- vals)
       {
         db.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
           .append("middle", new Document("codice_fiscale",  r(0).toString )))
         //log.info(s"Valore di split : ${r(0).toString}")

       }
       println(s"select distinct chiave from misure.elenco_splits where collection='FornitureElettriche'")


     }

     log.info("Estrazione forniture")

     val timeZone = prop.getProperty("spark.app.time_zone")
     val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
     val anno = Integer.toString(cal.get(Calendar.YEAR))
     val mese = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
     val giorno = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

     val d_max = anno + mese + giorno
     val d_min = (anno.toInt - 1).toString + mese + giorno


     val dt = hiveCtx.sql(
       s"""
        SELECT t_cf,codice_pod ,CONCAT(nvl(t_nome,'') ,'/b', nvl(t_cognome,'') ,'/b',nvl(t_piva,'') ,'/b',nvl(t_ragsoc,'')) anagrafica ,
        CONCAT(nvl(codice_fornitura,'') ,'/b',nvl(data_inizio_fornitura,'') ,'/b',nvl(data_fine_fornitura,'') ,'/b',nvl(tipo_mercato,'') ,'/b',nvl(residente,'') ,'/b',nvl(tariffa,'') ,'/b',
        nvl(tensione ,'') ,'/b', nvl(potenza_disponibile ,'') ,'/b', nvl(potenza_impegnata  ,''),'/b',nvl(stato_misuratore_2g  ,'') ,'/b',nvl(toponimo,'') ,'/b',
        nvl(nome_strada  ,'') ,'/b',nvl(civico ,'') ,'/b',nvl(comune ,'') ,'/b', nvl(cap ,'') ,'/b',nvl(provincia ,'') ,'/b',nvl(nazione ,'') ,'/b',nvl(trattamento ,'') ,'/b',
        nvl(data_inizio_processo_gdm ,'') ,'/b', nvl(data_fine_processo_gdm ,'') ,'/b',nvl(data_inizio_validita_gdm ,'') ,'/b',nvl(id_processo_gdm ,'') ,'/b',
        nvl(in_corso_gdm ,'') ,'/b',nvl(note_gdm  ,'') ,'/b',nvl(tipo_processo_gdm ,'') ,'/b',nvl(data_inizio_processo_switch ,'') ,'/b',nvl(data_fine_processo_switch ,'') ,'/b',
        nvl(data_inizio_validita_switch ,'') ,'/b',nvl(id_processo_switch ,'') ,'/b',nvl(in_corso_switch ,'') ,'/b',nvl(note_switch ,'') ,'/b',
        nvl(tipo_processo_switch,''),'/b',nvl(tipo_misuratore  ,'') ,'/b',nvl(matricola_misuratore,''),'/b',nvl(p_iva_cc,''),'/b',
        nvl(ragione_sociale_cc,''),'/b',nvl(ragione_sociale_distributore ,'')) dati,
        CONCAT(nvl(F_LUNEDI,''),'/b',nvl(F_MARTEDI,''),'/b',nvl(F_MERCOLEDI,''),'/b',nvl(F_GIOVEDI,''),'/b',
        nvl(F_VENERDI,''),'/b',nvl(F_SABATO,''),'/b',nvl(F_DOMENICA,''),'/b',nvl(F_FESTIVO,''),'/b',
        nvl(d_inizio_validita_fascia,''),'/b',nvl(d_fine_validita_fascia,''),'/b',nvl(d_data_iniziofreezing,''),'/b',nvl(id_misuratore_fasce,'')) fasce,data_inizio_fornitura_num
        FROM
        ( SELECT forniture.* FROM mongodbs.forniture_elettriche${suffix_forn} forniture
          left outer join (
          select t_cf,count(*) num from mongodbs.forniture_elettriche${suffix_forn} group by t_cf having count(*) > 20000
          ) as tbl ON forniture.t_cf = tbl.t_cf
          where tbl.t_cf is null
        ) AS forns where nvl(t_cf,'') <>''
        order by t_cf,codice_pod,data_inizio_fornitura_num
       """)



     val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString, row(3).toString, row(4).toString))
     val rdd = tp.groupByKey().map { case (x, y) => (x, (y.toList)) }


     val rdd_docs = rdd.map(x => {
       val dc = new BsonDocument()

       val codfisc = x._1
       val list = x._2


       val anagrafica = new BsonDocument()
       var anagrafica_ok = false
       val bsonArPods = new BsonArray()
       var dictPods = Map("" -> new BsonDocument())
       var dictPodForn = Map("" -> new BsonDocument())
       var dictPodForn_Fasce = Map("" -> "")
       var dictPodProc = Map("" -> "")

       for (i <- list) {
         val pod = i._1

         if (!anagrafica_ok) {
           val anagrafica_vv = i._2.split("/b", -1)
           anagrafica.
             append("cognome", new BsonString(anagrafica_vv(1))).
             append("nome", new BsonString(anagrafica_vv(0))).
             append("p_iva", new BsonString(anagrafica_vv(2))).
             append("ragione_sociale", new BsonString(anagrafica_vv(3)))

           dc.append("anagrafica", anagrafica)

           dc.append("codice_fiscale", new BsonString(codfisc))
           dc.append("id", new BsonString(randomUUID().toString))


           anagrafica_ok = true
         }
         val forn = i._3.split("/b", -1)
         val fasce = i._4.split("/b", -1)

         // ricavo i pod associati al codfiscale
         // uso una hashtable per memorizzare i pod che trovo
         val bson_pod_f = if (dictPods.contains(pod)) dictPods.get(pod).get
         else {
           val bsc_pod = new BsonDocument()
           dictPods += (pod -> bsc_pod)
           bsc_pod.append("codice_pod", new BsonString(pod))
           bsc_pod.append("forniture", new BsonArray())
           bsc_pod.append("processi", new BsonArray())

           bsc_pod
         }

         //normalizzo i dati della fornitura/processo

         val fn = Fornitura_Ele(forn(0), forn(1), forn(2), forn(3), forn(4),
           forn(5), forn(6), forn(7), forn(8), forn(9),
           forn(10), forn(11), forn(12), forn(13), forn(14),
           forn(15), forn(16), forn(17))

         val fn2 = Fornitura_Ele2(forn(32), forn(33), forn(34), forn(35), forn(36))

         val pc = Processo_Ele_Gas(forn(18), forn(19),
           forn(20), forn(21), forn(22), forn(23), forn(24),
           forn(25), forn(26), forn(27), forn(28), forn(29),
           forn(30), forn(31))

         val fasceC=FasceForniture_Ele(fasce(0),fasce(1),fasce(2),fasce(3),fasce(4),fasce(5),
           fasce(6),fasce(7),fasce(8),fasce(9),fasce(10),fasce(11))


         if (!dictPodForn.contains(pod + fn.cod_fornitura)) {
           //ottengo l'array delle forniture associate al pod
           //aggiungo la nuova fornitura
           val arrfor = bson_pod_f.getArray("forniture")
           val bs_forn = new BsonDocument()
           bs_forn.append("cap", new BsonString(fn.cap))
           bs_forn.append("civico", new BsonString(fn.civico))
           bs_forn.append("codice_fornitura", new BsonString(fn.cod_fornitura))
           bs_forn.append("comune", new BsonString(fn.comune))

           val data_fine=if(fn.data_fine_fornitura!="" && fn.data_fine_fornitura.length>=10 )fn.data_fine_fornitura.substring(0,4)+fn.data_fine_fornitura.substring(5,7)+fn.data_fine_fornitura.substring(8,10) else fn.data_fine_fornitura
           val data_inizio=if(fn.data_inizio_fornitura!="" && fn.data_inizio_fornitura.length>=10)fn.data_inizio_fornitura.substring(0,4)+fn.data_inizio_fornitura.substring(5,7)+fn.data_inizio_fornitura.substring(8,10)else fn.data_inizio_fornitura

           bs_forn.append("data_fine_fornitura", new BsonString(data_fine))
           bs_forn.append("data_inizio_fornitura", new BsonString(data_inizio))
           bs_forn.append("matricola_misuratore", new BsonString(fn2.matricola_misuratore))
           bs_forn.append("nazione", new BsonString(fn.nazione))
           bs_forn.append("nome_strada", new BsonString(fn.nome_strada))
           bs_forn.append("p_iva_cc", new BsonString(fn2.p_iva_cc))
           bs_forn.append("potenza_disponibile", new BsonString(fn.potenza_disponibile))
           bs_forn.append("potenza_impegnata", new BsonString(fn.potenza_impegnata))
           bs_forn.append("provincia", new BsonString(fn.provincia))
           bs_forn.append("ragione_sociale_cc", new BsonString(fn2.ragione_sociale_cc))
           bs_forn.append("ragione_sociale_distributore", new BsonString(fn2.ragione_sociale_distributore))
           bs_forn.append("residente", new BsonString(fn.residente))
           bs_forn.append("stato_misuratore_2G", new BsonString(fn.stato_misuratore_2g))
           bs_forn.append("tariffa", new BsonString(fn.tariffa))
           bs_forn.append("tensione", new BsonString(fn.tensione))
           bs_forn.append("tipo_mercato", new BsonString(fn.tipo_mercato))
           bs_forn.append("tipo_misuratore", new BsonString(fn2.tipo_misuratore))
           bs_forn.append("toponimo_Indirizzo", new BsonString(fn.toponimo))
           bs_forn.append("trattamento", new BsonString(fn.trattamento))



           //aggiungo le fasce per la fornitura in oggetto
           if(!(fasceC.d_inizio_validita_fascia=="" &&
             fasceC.d_fine_validita_fascia=="" && fasceC.d_data_iniziofreezing=="" && fasceC.f_lunedi=="")) {
             val bs_fascia = new BsonDocument()
             bs_fascia.append("Data_Inizio_Configurazione", new BsonString(fasceC.d_inizio_validita_fascia))
             bs_fascia.append("Data_Fine_Configurazione", new BsonString(fasceC.d_fine_validita_fascia))
             bs_fascia.append("Data_Freezing", new BsonString(fasceC.d_data_iniziofreezing))
             bs_fascia.append("f_domenica", new BsonString(fasceC.f_domenica))
             bs_fascia.append("f_festivo", new BsonString(fasceC.f_festivo))
             bs_fascia.append("f_giovedi", new BsonString(fasceC.f_giovedi))
             bs_fascia.append("f_lunedi", new BsonString(fasceC.f_lunedi))
             bs_fascia.append("f_martedi", new BsonString(fasceC.f_martedi))
             bs_fascia.append("f_mercoledi", new BsonString(fasceC.f_mercoledi))
             bs_fascia.append("f_sabato", new BsonString(fasceC.f_sabato))
             bs_fascia.append("f_venerdi", new BsonString(fasceC.f_venerdi))


             bs_forn.append("configurazione_fasce", new BsonArray())
             val arr_fasce = bs_forn.getArray("configurazione_fasce")
             arr_fasce.add(bs_fascia)
             dictPodForn_Fasce+= (pod + fn.cod_fornitura + fasceC.id_misuratore_fasce -> "ok")
           }

           arrfor.add(bs_forn)
           dictPodForn += (pod + fn.cod_fornitura -> bs_forn)
         }else{
           val bs_forn = dictPodForn.get(pod + fn.cod_fornitura).get

           if (!(fasceC.d_inizio_validita_fascia=="" &&
               fasceC.d_fine_validita_fascia=="" && fasceC.d_data_iniziofreezing=="" && fasceC.f_lunedi=="")
             && !dictPodForn_Fasce.contains(pod + fn.cod_fornitura + fasceC.id_misuratore_fasce) ) {
             val arr_fasce = bs_forn.getArray("configurazione_fasce")

             val bs_fascia = new BsonDocument()
             bs_fascia.append("Data_Inizio_Configurazione", new BsonString(fasceC.d_inizio_validita_fascia))
             bs_fascia.append("Data_Fine_Configurazione", new BsonString(fasceC.d_fine_validita_fascia))
             bs_fascia.append("Data_Freezing", new BsonString(fasceC.d_data_iniziofreezing))
             bs_fascia.append("f_domenica", new BsonString(fasceC.f_domenica))
             bs_fascia.append("f_festivo", new BsonString(fasceC.f_festivo))
             bs_fascia.append("f_giovedi", new BsonString(fasceC.f_giovedi))
             bs_fascia.append("f_lunedi", new BsonString(fasceC.f_lunedi))
             bs_fascia.append("f_martedi", new BsonString(fasceC.f_martedi))
             bs_fascia.append("f_mercoledi", new BsonString(fasceC.f_mercoledi))
             bs_fascia.append("f_sabato", new BsonString(fasceC.f_sabato))
             bs_fascia.append("f_venerdi", new BsonString(fasceC.f_venerdi))

             arr_fasce.add(bs_fascia)
             dictPodForn_Fasce+= (pod + fn.cod_fornitura + fasceC.id_misuratore_fasce -> "ok")

           }

         }


         if (pc.id_processo_gdm != "" && !dictPodProc.contains(pod + pc.id_processo_gdm)) {
           //ottengo l'array dei processi legati al pod
           //e aggiungo il processo gdm


           val arrProcessi = bson_pod_f.getArray("processi")
           val bs_proc = new BsonDocument()
           bs_proc.append("id_processo", new BsonString(pc.id_processo_gdm))
           bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_gdm))
           bs_proc.append("in_corso", new BsonString(pc.in_corso_gdm))
           bs_proc.append("data_inizio_processo", new BsonString(pc.data_inizio_processo_gdm))
           bs_proc.append("data_fine_processo", new BsonString(pc.data_fine_processo_gdm))
           bs_proc.append("note", new BsonString(pc.note_gdm))
           bs_proc.append("data_di_decorrenza", new BsonString(pc.data_inizio_validita_gdm))



           arrProcessi.add(bs_proc)

           dictPodProc += (pod + pc.id_processo_gdm -> "ok")
         }

         if (pc.id_processo_switch != "" && !dictPodProc.contains(pod + "_" + pc.id_processo_switch)) {
           //ottengo l'array dei processi legati al pod
           //e aggiungo il processo switch

           val arrProcessi = bson_pod_f.getArray("processi")
           val bs_proc = new BsonDocument()
           bs_proc.append("id_processo", new BsonString(pc.id_processo_switch))
           bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_switch))
           bs_proc.append("in_corso", new BsonString(pc.in_corso_switch))
           bs_proc.append("data_inizio_processo", new BsonString(pc.data_inizio_processo_switch))
           bs_proc.append("data_fine_processo", new BsonString(pc.data_fine_processo_switch))
           bs_proc.append("note", new BsonString(pc.note_switch))
           bs_proc.append("data_di_decorrenza", new BsonString(pc.data_inizio_validita_switch))



           arrProcessi.add(bs_proc)
           dictPodProc += (pod + "_" + pc.id_processo_switch -> "ok")
         }

       }

       for (bpod <- dictPods.values) {
         if (!bpod.isEmpty)
           bsonArPods.add(bpod)
       }

       dictPods.empty
       dictPodForn.empty
       dictPodProc.empty
       dictPodForn_Fasce.empty




       dc.append("pod", bsonArPods)

       dc


     }

     )


     log.info("Scrittura forniture elettriche")


     if(!mongocollaudo) {
       //istruzioni per droppare la collection
       try {
         val dt_clean = MongoSpark.read(hiveCtx).load().filter("codice_fiscale = 'NO'")
         MongoSpark.save(dt_clean.write.mode("overwrite"))
       } catch {
         case e: Exception => {}
       }
     }

     //per la versione 4.4. bisogna fare una seconda drop

    /* try{
     val data = MongoSpark.load(hiveCtx, ReadConfig(Map("collection" -> "TestFornitureEE"), Some(ReadConfig(hiveCtx))))
     MongoSpark.save(data.write.mode(SaveMode.Append))
     }catch {
       case e: Exception => {}
     }*/

      if(mongocollaudo)
        {
          val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
          val mongoClient = new MongoClient(new MongoClientURI(connString))
          val db = mongoClient.getDB(dbMongo)
          val c = db.getCollection(collectionMongo)
          var cicla=true
          var numcicli=0
          //al massimo attendo 50 minuti per verificare il bilanciamento
          while(cicla){
           val rtv =getShardDistribution(c)
            numcicli=numcicli+1
            if(rtv == -1 || rtv<=15 || numcicli>50)
              cicla=false
            else
              Thread.sleep(60000)
          }
        }

       val writeConfig = WriteConfig(Map("uri"->mongouri, "database" -> dbMongo,"collection" -> collectionMongo,"replicaSet"->"rset","writeConcern.w"->writeConcern_Write_Prod,"writeConcern.j"->writeConcern_Journal_Prod))
       /*if(!mongocollaudo)MongoSpark.save(rdd_docs, writeConfig)
       else*/ saveUnordered(rdd_docs,writeConfig)


   }


  def ExportFornitureElettrico2(dbMongo :String,collectionMongo:String): Unit = {

    log.info("Estrazione forniture aventi piu di 20000 pod/forniture")

    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    val d_max = anno + mese + giorno
    val d_min = (anno.toInt - 1).toString + mese + giorno


    val dt = hiveCtx.sql(
      s"""
        SELECT t_cf,codice_pod,CONCAT(nvl(t_nome,'') ,'/b', nvl(t_cognome,'') ,'/b',nvl(t_piva,'') ,'/b',nvl(t_ragsoc,'')) anagrafica ,
        CONCAT(nvl(codice_fornitura,'') ,'/b',nvl(data_inizio_fornitura,'') ,'/b',nvl(data_fine_fornitura,'') ,'/b',nvl(tipo_mercato,'') ,'/b',nvl(residente,'') ,'/b',nvl(tariffa,'') ,'/b',
        nvl(tensione ,'') ,'/b', nvl(potenza_disponibile ,'') ,'/b', nvl(potenza_impegnata  ,''),'/b',nvl(stato_misuratore_2g  ,'') ,'/b',nvl(toponimo,'') ,'/b',
        nvl(nome_strada  ,'') ,'/b',nvl(civico ,'') ,'/b',nvl(comune ,'') ,'/b', nvl(cap ,'') ,'/b',nvl(provincia ,'') ,'/b',nvl(nazione ,'') ,'/b',nvl(trattamento ,'') ,'/b',
        nvl(data_inizio_processo_gdm ,'') ,'/b', nvl(data_fine_processo_gdm ,'') ,'/b',nvl(data_inizio_validita_gdm ,'') ,'/b',nvl(id_processo_gdm ,'') ,'/b',
        nvl(in_corso_gdm ,'') ,'/b',nvl(note_gdm  ,'') ,'/b',nvl(tipo_processo_gdm ,'') ,'/b',nvl(data_inizio_processo_switch ,'') ,'/b',nvl(data_fine_processo_switch ,'') ,'/b',
        nvl(data_inizio_validita_switch ,'') ,'/b',nvl(id_processo_switch ,'') ,'/b',nvl(in_corso_switch ,'') ,'/b',nvl(note_switch ,'') ,'/b',
        nvl(tipo_processo_switch,''),'/b',nvl(tipo_misuratore  ,'') ,'/b',nvl(matricola_misuratore,''),'/b',nvl(p_iva_cc,''),'/b',
        nvl(ragione_sociale_cc,''),'/b',nvl(ragione_sociale_distributore ,'')) dati,
        CONCAT(nvl(F_LUNEDI,''),'/b',nvl(F_MARTEDI,''),'/b',nvl(F_MERCOLEDI,''),'/b',nvl(F_GIOVEDI,''),'/b',
        nvl(F_VENERDI,''),'/b',nvl(F_SABATO,''),'/b',nvl(F_DOMENICA,''),'/b',nvl(F_FESTIVO,''),'/b',
        nvl(d_inizio_validita_fascia,''),'/b',nvl(d_fine_validita_fascia,''),'/b',nvl(d_data_iniziofreezing,''),'/b',nvl(id_misuratore_fasce,'')) fasce,data_inizio_fornitura_num
        FROM
        ( SELECT forniture.* FROM mongodbs.forniture_elettriche${suffix_forn} forniture
          inner join (
          select t_cf,count(*) num from mongodbs.forniture_elettriche${suffix_forn} group by t_cf having count(*) > 20000
          ) as tbl ON forniture.t_cf = tbl.t_cf
        ) AS forns where nvl(t_cf,'') <>''
         order by t_cf,codice_pod,data_inizio_fornitura_num
       """)


    val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString, row(3).toString, row(4).toString))
    val rdd = tp.groupByKey().map { case (x, y) => (x, (y.toList)) }

    val numSplitting=10
    val maxcc=hiveCtx.sql(
      s"""
        select nvl(CAST(max(num)/${numSplitting} AS INT),0) maxcc
        from
          (
            select t_cf,count(*) num from mongodbs.forniture_elettriche group by t_cf having count(*) > 20000
          ) as tbl
      """.stripMargin
    ).collect()(0).getAs[Int](0)
    if(maxcc==0)
      {
        log.info("Non sono state trovate forniture elettriche aventi piu di 20000 pod/forniture")
        return
      }

    val rdd_docs = rdd.map(x => {
      var dc = new BsonDocument()

      val codfisc = x._1
      val list = x._2


      val anagrafica = new BsonDocument()
      var anagrafica_ok = false
      val bsonArPods = new BsonArray()
      var dictPods: Map[String, BsonDocument] = Map("" -> new BsonDocument())
      var dictPodForn: Map[String, BsonDocument] = Map("" -> new BsonDocument())
      var dictPodForn_Fasce: Map[String, String] = Map("" -> "")
      var dictPodProc: Map[String, String] = Map("" -> "")



      for (i <- list) {
        val pod = i._1

        if (!anagrafica_ok) {
          val anagrafica_vv = i._2.split("/b", -1)
          anagrafica.
            append("cognome", new BsonString(anagrafica_vv(1))).
            append("nome", new BsonString(anagrafica_vv(0))).
            append("p_iva", new BsonString(anagrafica_vv(2))).
            append("ragione_sociale", new BsonString(anagrafica_vv(3)))

          dc.append("anagrafica", anagrafica)

          dc.append("codice_fiscale", new BsonString(codfisc))
          dc.append("id", new BsonString(randomUUID().toString))


          anagrafica_ok = true
        }
        val forn = i._3.split("/b", -1)
        val fasce = i._4.split("/b", -1)

        // ricavo i pod associati al codfiscale
        // uso una hashtable per memorizzare i pod che trovo
        val bson_pod_f: BsonDocument = if (dictPods.contains(pod)) dictPods.get(pod).get
        else {
          val bsc_pod = new BsonDocument()
          dictPods += (pod -> bsc_pod)
          bsc_pod.append("codice_pod", new BsonString(pod))
          bsc_pod.append("forniture", new BsonArray())
          bsc_pod.append("processi", new BsonArray())



          bsc_pod
        }

        //normalizzo i dati della fornitura/processo

        val fn = Fornitura_Ele(forn(0), forn(1), forn(2), forn(3), forn(4),
          forn(5), forn(6), forn(7), forn(8), forn(9),
          forn(10), forn(11), forn(12), forn(13), forn(14),
          forn(15), forn(16), forn(17))

        val fn2 = Fornitura_Ele2(forn(32), forn(33), forn(34), forn(35), forn(36))

        val pc = Processo_Ele_Gas(forn(18), forn(19),
          forn(20), forn(21), forn(22), forn(23), forn(24),
          forn(25), forn(26), forn(27), forn(28), forn(29),
          forn(30), forn(31))

        val fasceC=FasceForniture_Ele(fasce(0),fasce(1),fasce(2),fasce(3),fasce(4),fasce(5),
          fasce(6),fasce(7),fasce(8),fasce(9),fasce(10),fasce(11))


        if (!dictPodForn.contains(pod + fn.cod_fornitura)) {
          //ottengo l'array delle forniture associate al pod
          //aggiungo la nuova fornitura
          val arrfor = bson_pod_f.getArray("forniture")
          val bs_forn = new BsonDocument()
          bs_forn.append("cap", new BsonString(fn.cap))
          bs_forn.append("civico", new BsonString(fn.civico))
          bs_forn.append("codice_fornitura", new BsonString(fn.cod_fornitura))
          bs_forn.append("comune", new BsonString(fn.comune))

          val data_fine=if(fn.data_fine_fornitura!="" && fn.data_fine_fornitura.length>=10 )fn.data_fine_fornitura.substring(0,4)+fn.data_fine_fornitura.substring(5,7)+fn.data_fine_fornitura.substring(8,10) else fn.data_fine_fornitura
          val data_inizio=if(fn.data_inizio_fornitura!="" && fn.data_inizio_fornitura.length>=10)fn.data_inizio_fornitura.substring(0,4)+fn.data_inizio_fornitura.substring(5,7)+fn.data_inizio_fornitura.substring(8,10)else fn.data_inizio_fornitura

          bs_forn.append("data_fine_fornitura", new BsonString(data_fine))
          bs_forn.append("data_inizio_fornitura", new BsonString(data_inizio))
          bs_forn.append("matricola_misuratore", new BsonString(fn2.matricola_misuratore))
          bs_forn.append("nazione", new BsonString(fn.nazione))
          bs_forn.append("nome_strada", new BsonString(fn.nome_strada))
          bs_forn.append("p_iva_cc", new BsonString(fn2.p_iva_cc))
          bs_forn.append("potenza_disponibile", new BsonString(fn.potenza_disponibile))
          bs_forn.append("potenza_impegnata", new BsonString(fn.potenza_impegnata))
          bs_forn.append("provincia", new BsonString(fn.provincia))
          bs_forn.append("ragione_sociale_cc", new BsonString(fn2.ragione_sociale_cc))
          bs_forn.append("ragione_sociale_distributore", new BsonString(fn2.ragione_sociale_distributore))
          bs_forn.append("residente", new BsonString(fn.residente))
          bs_forn.append("stato_misuratore_2G", new BsonString(fn.stato_misuratore_2g))
          bs_forn.append("tariffa", new BsonString(fn.tariffa))
          bs_forn.append("tensione", new BsonString(fn.tensione))
          bs_forn.append("tipo_mercato", new BsonString(fn.tipo_mercato))
          bs_forn.append("tipo_misuratore", new BsonString(fn2.tipo_misuratore))
          bs_forn.append("toponimo_Indirizzo", new BsonString(fn.toponimo))
          bs_forn.append("trattamento", new BsonString(fn.trattamento))



          //aggiungo le fasce per la fornitura in oggetto
          if(!(fasceC.d_inizio_validita_fascia=="" &&
            fasceC.d_fine_validita_fascia=="" && fasceC.d_data_iniziofreezing=="" && fasceC.f_lunedi=="")){
            val bs_fascia = new BsonDocument()
            bs_fascia.append("Data_Inizio_Configurazione", new BsonString(fasceC.d_inizio_validita_fascia))
            bs_fascia.append("Data_Fine_Configurazione", new BsonString(fasceC.d_fine_validita_fascia))
            bs_fascia.append("Data_Freezing", new BsonString(fasceC.d_data_iniziofreezing))
            bs_fascia.append("f_domenica", new BsonString(fasceC.f_domenica))
            bs_fascia.append("f_festivo", new BsonString(fasceC.f_festivo))
            bs_fascia.append("f_giovedi", new BsonString(fasceC.f_giovedi))
            bs_fascia.append("f_lunedi", new BsonString(fasceC.f_lunedi))
            bs_fascia.append("f_martedi", new BsonString(fasceC.f_martedi))
            bs_fascia.append("f_mercoledi", new BsonString(fasceC.f_mercoledi))
            bs_fascia.append("f_sabato", new BsonString(fasceC.f_sabato))
            bs_fascia.append("f_venerdi", new BsonString(fasceC.f_venerdi))


            bs_forn.append("configurazione_fasce", new BsonArray())
            val arr_fasce = bs_forn.getArray("configurazione_fasce")
            arr_fasce.add(bs_fascia)
            dictPodForn_Fasce+= (pod + fn.cod_fornitura + fasceC.id_misuratore_fasce -> "ok")
          }

          arrfor.add(bs_forn)
          dictPodForn += (pod + fn.cod_fornitura -> bs_forn)
        }
        else{
          val bs_forn:BsonDocument = dictPodForn.get(pod + fn.cod_fornitura).get

          if (!(fasceC.d_inizio_validita_fascia=="" &&
              fasceC.d_fine_validita_fascia=="" && fasceC.d_data_iniziofreezing=="" && fasceC.f_lunedi=="")
            && !dictPodForn_Fasce.contains(pod + fn.cod_fornitura + fasceC.id_misuratore_fasce)) {
            val arr_fasce = bs_forn.getArray("configurazione_fasce")

            val bs_fascia = new BsonDocument()
            bs_fascia.append("Data_Inizio_Configurazione", new BsonString(fasceC.d_inizio_validita_fascia))
            bs_fascia.append("Data_Fine_Configurazione", new BsonString(fasceC.d_fine_validita_fascia))
            bs_fascia.append("Data_Freezing", new BsonString(fasceC.d_data_iniziofreezing))
            bs_fascia.append("f_domenica", new BsonString(fasceC.f_domenica))
            bs_fascia.append("f_festivo", new BsonString(fasceC.f_festivo))
            bs_fascia.append("f_giovedi", new BsonString(fasceC.f_giovedi))
            bs_fascia.append("f_lunedi", new BsonString(fasceC.f_lunedi))
            bs_fascia.append("f_martedi", new BsonString(fasceC.f_martedi))
            bs_fascia.append("f_mercoledi", new BsonString(fasceC.f_mercoledi))
            bs_fascia.append("f_sabato", new BsonString(fasceC.f_sabato))
            bs_fascia.append("f_venerdi", new BsonString(fasceC.f_venerdi))

            arr_fasce.add(bs_fascia)
            dictPodForn_Fasce+= (pod + fn.cod_fornitura + fasceC.id_misuratore_fasce -> "ok")

          }

        }

        if (pc.id_processo_gdm != "" && !dictPodProc.contains(pod + pc.id_processo_gdm)) {
          //ottengo l'array dei processi legati al pod
          //e aggiungo il processo gdm


          val arrProcessi = bson_pod_f.getArray("processi")
          val bs_proc = new BsonDocument()
          bs_proc.append("id_processo", new BsonString(pc.id_processo_gdm))
          bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_gdm))
          bs_proc.append("in_corso", new BsonString(pc.in_corso_gdm))
          bs_proc.append("data_inizio_processo", new BsonString(pc.data_inizio_processo_gdm))
          bs_proc.append("data_fine_processo", new BsonString(pc.data_fine_processo_gdm))
          bs_proc.append("note", new BsonString(pc.note_gdm))
          bs_proc.append("data_di_decorrenza", new BsonString(pc.data_inizio_validita_gdm))



          arrProcessi.add(bs_proc)

          dictPodProc += (pod + pc.id_processo_gdm -> "ok")
        }

        if (pc.id_processo_switch != "" && !dictPodProc.contains(pod + "_" + pc.id_processo_switch)) {
          //ottengo l'array dei processi legati al pod
          //e aggiungo il processo switch

          val arrProcessi = bson_pod_f.getArray("processi")
          val bs_proc = new BsonDocument()
          bs_proc.append("id_processo", new BsonString(pc.id_processo_switch))
          bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_switch))
          bs_proc.append("in_corso", new BsonString(pc.in_corso_switch))
          bs_proc.append("data_inizio_processo", new BsonString(pc.data_inizio_processo_switch))
          bs_proc.append("data_fine_processo", new BsonString(pc.data_fine_processo_switch))
          bs_proc.append("note", new BsonString(pc.note_switch))
          bs_proc.append("data_di_decorrenza", new BsonString(pc.data_inizio_validita_switch))



          arrProcessi.add(bs_proc)
          dictPodProc += (pod + "_" + pc.id_processo_switch -> "ok")
        }

      }

      var cc=0

      var sq=Seq[BsonDocument]()
      for (bpod <- dictPods.values) {
        if (!bpod.isEmpty)
          {
            val arrfor = bpod.getArray("forniture")
            cc=cc + arrfor.size()

            bsonArPods.add(bpod)
            if(cc>=maxcc)
              {

                dc.append("pod", bsonArPods)
                sq=sq :+ dc.clone()

                bsonArPods.clear()

                dc = new BsonDocument()
                dc.append("anagrafica", anagrafica)
                dc.append("codice_fiscale", new BsonString(codfisc))
                dc.append("id", new BsonString(randomUUID().toString))

                cc=0
              }



          }
      }




      if(!bsonArPods.isEmpty){
        dc.append("pod", bsonArPods)
        sq=sq :+ dc.clone()
      }
      dictPods.empty
      dictPodForn.empty
      dictPodProc.empty
      dictPodForn_Fasce.empty


      sq


    }

    )


    log.info("Scrittura forniture elettriche aventi piu di 20000 pod/forniture")

    var cicla =true
    for(ind <- 0 to (numSplitting +1)){

      val rdd_tmp=rdd_docs.map(f =>{if(f.length>ind)(f(ind)) else null}).filter(f => f != null)
      if(!rdd_tmp.partitions.isEmpty)
        writeDocsToMongoDb(rdd_tmp, dbMongo, collectionMongo)
    }



  }

  def writeDocsToMongoDb(rdd: RDD[BsonDocument], dbMongo :String, collectionMongo:String): Unit ={

      val writeConfig = WriteConfig(Map("uri"->mongouri, "database" -> dbMongo,"collection" -> collectionMongo,"replicaSet"->"rset","writeConcern.w"->writeConcern_Write_Prod,"writeConcern.j"->writeConcern_Journal_Prod))

    /*if(!mongocollaudo)MongoSpark.save(rdd, writeConfig)
    else*/ saveUnordered(rdd,writeConfig)

  }

  def ExportFornitureGas(dbMongo :String,collectionMongo:String): Unit ={

    if (mongocollaudo) {

      log.info(s"Impostazione collection ${dbMongo}.${collectionMongo} in modalità shard")
      val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
      val mongoClient: MongoClient = new MongoClient(new MongoClientURI(connString))
      val db = mongoClient.getDatabase("admin");


      //comandi per creare la collection in shard
      val dbx = mongoClient.getDatabase(dbMongo);
      /*val coll_old = dbx.getCollection(collectionMongo);
      coll_old.drop()
      coll_old.drop()
      Thread.sleep(5000)*/

      dbx.createCollection(collectionMongo)

      val documentA = db.runCommand(new Document("shardcollection", s"${dbMongo}.${collectionMongo}").append("key", new Document("codice_fiscale", 1)))

      log.info(s"Esecuzione split sulla collection ${dbMongo}.${collectionMongo}")

      val vals = hiveCtx.sql(s"select distinct chiave from misure.elenco_splits where collection='FornitureGas'").collect()

      for(r <- vals)
      {
        db.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
          .append("middle", new Document("codice_fiscale",  r(0).toString )))
       // log.info(s"Valore di split : ${r(0).toString}")
      }
      println(s"select distinct chiave from misure.elenco_splits where collection='FornitureGas'")

    }

    log.info("Estrazione forniture gas")

    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    val d_max = anno + mese + giorno
    val d_min = (anno.toInt - 1).toString + mese + giorno

    val dt = hiveCtx.sql(
      s"""
       SELECT t_codice_fiscale,nvl(forns.t_codice_pdr,'')t_codice_pdr ,CONCAT(nvl(t_nome,'') ,'/b', nvl(t_cognome,'') ,'/b',nvl(t_partita_iva,'') ,'/b',nvl(t_ragione_sociale,'')) anagrafica ,
       CONCAT(nvl(t_cap ,'') ,'/b',nvl(categoria_duso ,''),'/b',nvl(t_civico ,''),'/b',nvl(t_classe_misuratore ,''),'/b',nvl(codice_fornitura,'') , '/b',
       nvl(coefficiente_conversione,'') , '/b',nvl(t_comune,'') , '/b',
       nvl(data_fine_fornitura,'') ,'/b',nvl(data_inizio_fornitura,''),'/b',nvl(matricola_misuratore,''),'/b',nvl(t_nazione ,'') ,'/b',
       nvl(t_nomestrada ,'') ,'/b',nvl(piva_cc,''),'/b',nvl(t_provincia ,'') ,'/b',nvl(ragione_sociale_cc,''),'/b',
       nvl(ragione_sociale_distributore ,''),'/b',
       nvl(residente,'') ,'/b', nvl(tipo_fornitura,'') ,'/b', nvl(tipo_pdr ,'') ,'/b',nvl(toponimo_indirizzo ,'')) dati,
       CONCAT(nvl(data_inizio_processo_gdm ,'') ,'/b', nvl(data_fine_processo_gdm ,'') ,'/b',nvl(data_inizio_validita_gdm ,'') ,'/b',nvl(id_processo_gdm ,'') ,'/b',
        nvl(in_corso_gdm ,'') ,'/b',nvl(note_gdm  ,'') ,'/b',nvl(tipo_processo_gdm ,'') ,'/b',nvl(PG.data_inizio_processo_switch ,'') ,'/b',nvl(PG.data_fine_processo_switch ,'') ,'/b',
        nvl(PG.data_inizio_validita_switch ,'') ,'/b',nvl(PG.id_processo_switch ,'') ,'/b',nvl(PG.in_corso_switch ,'') ,'/b',nvl(PG.note_switch ,'') ,'/b',
        nvl(PG.tipo_processo_switch,''))processi,data_inizio_fornitura_num

       FROM
        ( SELECT forniture.* FROM ${dbgas}.forniture_gas${suffix_forn} forniture
          left outer join (
          select t_codice_fiscale,count(*) num from ${dbgas}.forniture_gas${suffix_forn} group by t_codice_fiscale having count(*) > 20000
          ) as tbl ON forniture.t_codice_fiscale = tbl.t_codice_fiscale
          where nvl(forniture.t_codice_fiscale,'') <>'' and  tbl.t_codice_fiscale is  null
        ) AS forns
         left outer join ${dbgas}.ProcessiGas${suffix_forn} PG ON PG.t_codice_pdr = forns.t_codice_pdr
         where nvl(t_codice_fiscale,'') <>''
         order by t_codice_fiscale,t_codice_pdr,data_inizio_fornitura_num
       """)




    val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString,row(3).toString,row(4).toString))
    val rdd=tp.groupByKey().map{ case(x, y) => (x, (y.toList)) }


    val rdd_docs = rdd.map( x => {
      val dc = new BsonDocument()

      val codfisc = x._1
      val list = x._2


      val anagrafica = new BsonDocument()
      var anagrafica_ok = false
      val bsonArPdrs = new BsonArray()
      var dictPdrs: Map[String, BsonDocument] = Map("" -> new BsonDocument())
      var dictPdrForn: Map[String, String] = Map("" -> "")
      var dictPdrProc: Map[String, String] = Map("" -> "")




      for (i <- list) {
        val pdr = i._1

        if (pdr.trim != "") {


          if (!anagrafica_ok) {
            val anagrafica_vv = i._2.split("/b", -1)
            anagrafica.
              append("cognome", new BsonString(anagrafica_vv(1))).
              append("nome", new BsonString(anagrafica_vv(0))).
              append("p_iva", new BsonString(anagrafica_vv(2))).
              append("ragione_sociale", new BsonString(anagrafica_vv(3)))

            dc.append("anagrafica", anagrafica)

            dc.append("codice_fiscale", new BsonString(codfisc))

            dc.append("id", new BsonString(randomUUID().toString))


            anagrafica_ok = true
          }
          val forn = i._3.split("/b", -1)
          val processi = i._4.split("/b", -1)


          // ricavo i pod associati al codfiscale
          // uso una hashtable per memorizzare i pod che trovo
          val bson_pod_f: BsonDocument = if (dictPdrs.contains(pdr)) dictPdrs.get(pdr).get
          else {
            val bsc_pod = new BsonDocument()
            dictPdrs += (pdr -> bsc_pod)
            bsc_pod.append("codice_pdr", new BsonString(pdr))
            bsc_pod.append("forniture", new BsonArray())
            bsc_pod.append("processi", new BsonArray())


            bsc_pod
          }

          //normalizzo i dati della fornitura/processo


          val fn = FornituraGas(forn(0), forn(1), forn(2), forn(3), forn(4),
            forn(5), forn(6), forn(7), forn(8), forn(9),
            forn(10), forn(11), forn(12), forn(13), forn(14),
            forn(15), forn(16), forn(17), forn(18), forn(19))


          if (!dictPdrForn.contains(pdr + fn.codice_fornitura)) {
            //ottengo l'array delle forniture associate al pod
            //aggiungo la nuova fornitura
            val arrfor = bson_pod_f.getArray("forniture")
            val bs_forn = new BsonDocument()
            bs_forn.append("cap", new BsonString(fn.t_cap))
            bs_forn.append("categoria_uso", new BsonString(fn.categoria_duso))
            bs_forn.append("civico", new BsonString(fn.civico))
            bs_forn.append("classe_misuratore", new BsonString(fn.classe_misuratore))
            bs_forn.append("codice_fornitura", new BsonString(fn.codice_fornitura))
            bs_forn.append("coefficiente_conversione", new BsonString(fn.coefficiente_conversione))
            bs_forn.append("comune", new BsonString(fn.t_comune))
            val data_fine = if (fn.data_fine_fornitura != "" && fn.data_fine_fornitura.length >= 10) fn.data_fine_fornitura.substring(0, 4) + fn.data_fine_fornitura.substring(5, 7) + fn.data_fine_fornitura.substring(8, 10) else fn.data_fine_fornitura
            bs_forn.append("data_fine_fornitura", new BsonString(data_fine))
            val data_inizio = if (fn.data_inizio_fornitura != "" && fn.data_inizio_fornitura.length >= 10) fn.data_inizio_fornitura.substring(0, 4) + fn.data_inizio_fornitura.substring(5, 7) + fn.data_inizio_fornitura.substring(8, 10) else fn.data_inizio_fornitura
            bs_forn.append("data_inizio_fornitura", new BsonString(data_inizio))
            bs_forn.append("matricola_misuratore", new BsonString(fn.matricola_misuratore))
            bs_forn.append("nazione", new BsonString(fn.nazione))
            bs_forn.append("nome_strada", new BsonString(fn.nome_strada))
            bs_forn.append("p_iva_cc", new BsonString(fn.piva_cc))
            bs_forn.append("provincia", new BsonString(fn.provincia))
            bs_forn.append("ragione_sociale_cc", new BsonString(fn.ragione_sociale_cc))
            bs_forn.append("ragione_sociale_distributore", new BsonString(fn.ragione_sociale_distributore))
            bs_forn.append("residente", new BsonString(fn.residente))
            bs_forn.append("tipo_fornitura", new BsonString(fn.tipo_fornitura))
            bs_forn.append("tipo_pdr", new BsonString(fn.tipo_pdr))
            bs_forn.append("toponimo_Indirizzo", new BsonString(fn.toponimo_indirizzo))


            arrfor.add(bs_forn)
            dictPdrForn += (pdr + fn.codice_fornitura -> "ok")
          }



          val pc = Processo_Ele_Gas(processi(0), processi(1),
            processi(2), processi(3), processi(4), processi(5), processi(6),
            processi(7), processi(8), processi(9), processi(10), processi(11),
            processi(12), processi(13))

          if (pc.id_processo_gdm != "" && !dictPdrProc.contains(pdr + pc.id_processo_gdm)) {
            //ottengo l'array dei processi legati al pdr
            //e aggiungo il processo gdm


            val arrProcessi = bson_pod_f.getArray("processi")
            val bs_proc = new BsonDocument()
            bs_proc.append("id_processo", new BsonString(pc.id_processo_gdm))
            bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_gdm))
            bs_proc.append("in_corso", new BsonString(pc.in_corso_gdm))
            bs_proc.append("data_inizio_processo", new BsonString((if(pc.data_inizio_processo_gdm!="")pc.data_inizio_processo_gdm.replace("-","").substring(0,8) else "")))
            bs_proc.append("data_fine_processo", new BsonString((if(pc.data_fine_processo_gdm!="")pc.data_fine_processo_gdm.replace("-","").substring(0,8) else "")))
            bs_proc.append("note", new BsonString(pc.note_gdm))
            bs_proc.append("data_di_decorrenza", new BsonString((if(pc.data_inizio_validita_gdm!="")pc.data_inizio_validita_gdm.replace("-","").substring(0,8) else "")))


            arrProcessi.add(bs_proc)

            dictPdrProc += (pdr + pc.id_processo_gdm -> "ok")
          }

          if (pc.id_processo_switch != "" && !dictPdrProc.contains(pdr + "_" + pc.id_processo_switch)) {
            //ottengo l'array dei processi legati al pdr
            //e aggiungo il processo switch


            val arrProcessi = bson_pod_f.getArray("processi")
            val bs_proc = new BsonDocument()
            bs_proc.append("id_processo", new BsonString(pc.id_processo_switch))
            bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_switch))
            bs_proc.append("in_corso", new BsonString(pc.in_corso_switch))
            bs_proc.append("data_inizio_processo", new BsonString((if(pc.data_inizio_processo_switch!="")pc.data_inizio_processo_switch.replace("-","").substring(0,8) else "")))
            bs_proc.append("data_fine_processo", new BsonString((if(pc.data_fine_processo_switch!="")pc.data_fine_processo_switch.replace("-","").substring(0,8) else "")))
            bs_proc.append("note", new BsonString(pc.note_switch))
            bs_proc.append("data_di_decorrenza", new BsonString((if(pc.data_inizio_validita_switch!="")pc.data_inizio_validita_switch.replace("-","").substring(0,8) else "")))


            arrProcessi.add(bs_proc)
            dictPdrProc += (pdr + "_" + pc.id_processo_switch -> "ok")
          }
        }
      }

      for (bpod <- dictPdrs.values) {
        if (!bpod.isEmpty)
          bsonArPdrs.add(bpod)
      }


      dictPdrs.empty
      dictPdrForn.empty
      dictPdrProc.empty

      dc.append("pdr", bsonArPdrs)
      dc
    }

    )


    log.info("Scrittura forniture gas")


    if(!mongocollaudo) {
      //istruzioni per droppare la collection
      try {
        val dt_clean = MongoSpark.read(hiveCtx).load().filter("codice_fiscale = 'NO'")
        MongoSpark.save(dt_clean.write.mode("overwrite"))
      } catch {
        case e: Exception => {}
      }
    }

    //per la versione 4.4. bisogna fare una seconda drop

    /*val docTemplate: org.mongodb.scala.bson.Document = org.mongodb.scala.bson.Document("_id" -> 0,
      "anagrafica" -> org.mongodb.scala.bson.Document("cognome" -> "", "nome" -> "","p_iva" -> "","ragione_sociale" -> ""),"codice_fiscale" -> "CF2","id"-> "0000000000000000","pdr" -> org.mongodb.scala.bson.BsonArray())

    (dbMongo,collectionMongo,docTemplate,"codice_fiscale")*/


   /* try {
      val data = MongoSpark.load(hiveCtx, ReadConfig(Map("collection" -> "TestFornitureGas"), Some(ReadConfig(hiveCtx))))
      MongoSpark.save(data.write.mode(SaveMode.Append))
    }catch {
      case e: Exception => {}
    }*/


    if(mongocollaudo)
    {
      val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
      val mongoClient = new MongoClient(new MongoClientURI(connString))
      val db = mongoClient.getDB(dbMongo)
      val c = db.getCollection(collectionMongo)
      var cicla=true
      var numcicli=0
      //al massimo attendo 50 minuti per verificare il bilanciamento
      while(cicla){
        val rtv =getShardDistribution(c)
        numcicli=numcicli+1
        if(rtv == -1 || rtv<=15 || numcicli>50)
          cicla=false
        else
          Thread.sleep(60000)
      }
    }

      val writeConfig = WriteConfig(Map("uri"->mongouri, "database" -> dbMongo,"collection" -> collectionMongo,"replicaSet"->"rset","writeConcern.w"->writeConcern_Write_Prod,"writeConcern.j"->writeConcern_Journal_Prod))
      /*if(!mongocollaudo)MongoSpark.save(rdd_docs, writeConfig)
      else*/ saveUnordered(rdd_docs,writeConfig)

  }



  def saveUnordered[D: scala.reflect.ClassTag](rdd: RDD[D], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: com.mongodb.client.MongoCollection[D] =>
       { iter.grouped(512).foreach(batch => collection.insertMany(batch.toList.asJava,new InsertManyOptions().ordered(false))) }
      })
    })
  }




  def ExportFornitureGas2(dbMongo :String,collectionMongo:String): Unit ={

    log.info("Estrazione forniture gas aventi piu di 20000 pod/forniture")

    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
    val anno: String = Integer.toString(cal.get(Calendar.YEAR))
    val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
    val giorno: String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2

    val d_max = anno + mese + giorno
    val d_min = (anno.toInt - 1).toString + mese + giorno

    val dt = hiveCtx.sql(
      s"""
       SELECT t_codice_fiscale,nvl(forns.t_codice_pdr,'')t_codice_pdr ,CONCAT(nvl(t_nome,'') ,'/b', nvl(t_cognome,'') ,'/b',nvl(t_partita_iva,'') ,'/b',nvl(t_ragione_sociale,'')) anagrafica ,
       CONCAT(nvl(t_cap ,'') ,'/b',nvl(categoria_duso ,''),'/b',nvl(t_civico ,''),'/b',nvl(t_classe_misuratore ,''),'/b',nvl(codice_fornitura,'') , '/b',
       nvl(coefficiente_conversione,'') , '/b',nvl(t_comune,'') , '/b',
       nvl(data_fine_fornitura,'') ,'/b',nvl(data_inizio_fornitura,''),'/b',nvl(matricola_misuratore,''),'/b',nvl(t_nazione ,'') ,'/b',
       nvl(t_nomestrada ,'') ,'/b',nvl(piva_cc,''),'/b',nvl(t_provincia ,'') ,'/b',nvl(ragione_sociale_cc,''),'/b',
       nvl(ragione_sociale_distributore ,''),'/b',
       nvl(residente,'') ,'/b', nvl(tipo_fornitura,'') ,'/b', nvl(tipo_pdr ,'') ,'/b',nvl(toponimo_indirizzo ,'')) dati,
       CONCAT(nvl(data_inizio_processo_gdm ,'') ,'/b', nvl(data_fine_processo_gdm ,'') ,'/b',nvl(data_inizio_validita_gdm ,'') ,'/b',nvl(id_processo_gdm ,'') ,'/b',
        nvl(in_corso_gdm ,'') ,'/b',nvl(note_gdm  ,'') ,'/b',nvl(tipo_processo_gdm ,'') ,'/b',nvl(PG.data_inizio_processo_switch ,'') ,'/b',nvl(PG.data_fine_processo_switch ,'') ,'/b',
        nvl(PG.data_inizio_validita_switch ,'') ,'/b',nvl(PG.id_processo_switch ,'') ,'/b',nvl(PG.in_corso_switch ,'') ,'/b',nvl(PG.note_switch ,'') ,'/b',
        nvl(PG.tipo_processo_switch,''))processi,data_inizio_fornitura_num

       FROM
        ( SELECT forniture.* FROM ${dbgas}.forniture_gas${suffix_forn} forniture
          inner join (
          select t_codice_fiscale,count(*) num from ${dbgas}.forniture_gas${suffix_forn} group by t_codice_fiscale having count(*) > 20000
          ) as tbl ON forniture.t_codice_fiscale = tbl.t_codice_fiscale
          where nvl(forniture.t_codice_fiscale,'') <>''
        ) AS forns
         left outer join ${dbgas}.ProcessiGas${suffix_forn} PG ON PG.t_codice_pdr = forns.t_codice_pdr
         where nvl(t_codice_fiscale,'') <>''
         order by t_codice_fiscale,t_codice_pdr,data_inizio_fornitura_num
       """)



    val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString,row(3).toString,row(4).toString))
    val rdd=tp.groupByKey().map{ case(x, y) => (x, (y.toList)) }

    val numSplitting=10
    val maxcc=hiveCtx.sql(
      s"""
        select nvl(CAST(max(num)/${numSplitting} AS INT),0) maxcc
        from
          (
            select t_codice_fiscale,count(*) num from ${dbgas}.forniture_gas where nvl(t_codice_fiscale,'') <>'' group by t_codice_fiscale having count(*) > 20000
          ) as tbl
      """.stripMargin
    ).collect()(0).getAs[Int](0)

    if(maxcc==0)
    {
      log.info("Non sono state trovate forniture gas aventi piu di 20000 pod/forniture")
      return
    }

    val rdd_docs = rdd.map( x => {
      var dc = new BsonDocument()

      val codfisc = x._1
      val list = x._2


      val anagrafica = new BsonDocument()
      var anagrafica_ok = false
      val bsonArPdrs = new BsonArray()
      var dictPdrs: Map[String, BsonDocument] = Map("" -> new BsonDocument())
      var dictPdrForn: Map[String, String] = Map("" -> "")
      var dictPdrProc: Map[String, String] = Map("" -> "")




      for (i <- list) {
        val pdr = i._1

        if (pdr.trim != "") {


          if (!anagrafica_ok) {
            val anagrafica_vv = i._2.split("/b", -1)
            anagrafica.
              append("cognome", new BsonString(anagrafica_vv(1))).
              append("nome", new BsonString(anagrafica_vv(0))).
              append("p_iva", new BsonString(anagrafica_vv(2))).
              append("ragione_sociale", new BsonString(anagrafica_vv(3)))

            dc.append("anagrafica", anagrafica)

            dc.append("codice_fiscale", new BsonString(codfisc))

            dc.append("id", new BsonString(randomUUID().toString))


            anagrafica_ok = true
          }
          val forn = i._3.split("/b", -1)
          val processi = i._4.split("/b", -1)


          // ricavo i pod associati al codfiscale
          // uso una hashtable per memorizzare i pod che trovo
          val bson_pod_f: BsonDocument = if (dictPdrs.contains(pdr)) dictPdrs.get(pdr).get
          else {
            val bsc_pod = new BsonDocument()
            dictPdrs += (pdr -> bsc_pod)
            bsc_pod.append("codice_pdr", new BsonString(pdr))
            bsc_pod.append("forniture", new BsonArray())
            bsc_pod.append("processi", new BsonArray())


            bsc_pod
          }

          //normalizzo i dati della fornitura/processo


          val fn = FornituraGas(forn(0), forn(1), forn(2), forn(3), forn(4),
            forn(5), forn(6), forn(7), forn(8), forn(9),
            forn(10), forn(11), forn(12), forn(13), forn(14),
            forn(15), forn(16), forn(17), forn(18), forn(19))


          if (!dictPdrForn.contains(pdr + fn.codice_fornitura)) {
            //ottengo l'array delle forniture associate al pod
            //aggiungo la nuova fornitura
            val arrfor = bson_pod_f.getArray("forniture")
            val bs_forn = new BsonDocument()
            bs_forn.append("cap", new BsonString(fn.t_cap))
            bs_forn.append("categoria_uso", new BsonString(fn.categoria_duso))
            bs_forn.append("civico", new BsonString(fn.civico))
            bs_forn.append("classe_misuratore", new BsonString(fn.classe_misuratore))
            bs_forn.append("codice_fornitura", new BsonString(fn.codice_fornitura))
            bs_forn.append("coefficiente_conversione", new BsonString(fn.coefficiente_conversione))
            bs_forn.append("comune", new BsonString(fn.t_comune))
            val data_fine = if (fn.data_fine_fornitura != "" && fn.data_fine_fornitura.length >= 10) fn.data_fine_fornitura.substring(0, 4) + fn.data_fine_fornitura.substring(5, 7) + fn.data_fine_fornitura.substring(8, 10) else fn.data_fine_fornitura
            bs_forn.append("data_fine_fornitura", new BsonString(data_fine))
            val data_inizio = if (fn.data_inizio_fornitura != "" && fn.data_inizio_fornitura.length >= 10) fn.data_inizio_fornitura.substring(0, 4) + fn.data_inizio_fornitura.substring(5, 7) + fn.data_inizio_fornitura.substring(8, 10) else fn.data_inizio_fornitura
            bs_forn.append("data_inizio_fornitura", new BsonString(data_inizio))
            bs_forn.append("matricola_misuratore", new BsonString(fn.matricola_misuratore))
            bs_forn.append("nazione", new BsonString(fn.nazione))
            bs_forn.append("nome_strada", new BsonString(fn.nome_strada))
            bs_forn.append("p_iva_cc", new BsonString(fn.piva_cc))
            bs_forn.append("provincia", new BsonString(fn.provincia))
            bs_forn.append("ragione_sociale_cc", new BsonString(fn.ragione_sociale_cc))
            bs_forn.append("ragione_sociale_distributore", new BsonString(fn.ragione_sociale_distributore))
            bs_forn.append("residente", new BsonString(fn.residente))
            bs_forn.append("tipo_fornitura", new BsonString(fn.tipo_fornitura))
            bs_forn.append("tipo_pdr", new BsonString(fn.tipo_pdr))
            bs_forn.append("toponimo_Indirizzo", new BsonString(fn.toponimo_indirizzo))


            arrfor.add(bs_forn)
            dictPdrForn += (pdr + fn.codice_fornitura -> "ok")
          }



          val pc = Processo_Ele_Gas(processi(0), processi(1),
            processi(2), processi(3), processi(4), processi(5), processi(6),
            processi(7), processi(8), processi(9), processi(10), processi(11),
            processi(12), processi(13))

          if (pc.id_processo_gdm != "" && !dictPdrProc.contains(pdr + pc.id_processo_gdm)) {
            //ottengo l'array dei processi legati al pdr
            //e aggiungo il processo gdm


            val arrProcessi = bson_pod_f.getArray("processi")
            val bs_proc = new BsonDocument()
            bs_proc.append("id_processo", new BsonString(pc.id_processo_gdm))
            bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_gdm))
            bs_proc.append("in_corso", new BsonString(pc.in_corso_gdm))
            bs_proc.append("data_inizio_processo", new BsonString((if(pc.data_inizio_processo_gdm!="")pc.data_inizio_processo_gdm.replace("-","").substring(0,8) else "")))
            bs_proc.append("data_fine_processo", new BsonString((if(pc.data_fine_processo_gdm!="")pc.data_fine_processo_gdm.replace("-","").substring(0,8) else "")))
            bs_proc.append("note", new BsonString(pc.note_gdm))
            bs_proc.append("data_di_decorrenza", new BsonString((if(pc.data_inizio_validita_gdm!="")pc.data_inizio_validita_gdm.replace("-","").substring(0,8) else "")))


            arrProcessi.add(bs_proc)

            dictPdrProc += (pdr + pc.id_processo_gdm -> "ok")
          }

          if (pc.id_processo_switch != "" && !dictPdrProc.contains(pdr + "_" + pc.id_processo_switch)) {
            //ottengo l'array dei processi legati al pdr
            //e aggiungo il processo switch


            val arrProcessi = bson_pod_f.getArray("processi")
            val bs_proc = new BsonDocument()
            bs_proc.append("id_processo", new BsonString(pc.id_processo_switch))
            bs_proc.append("tipo_processo", new BsonString(pc.tipo_processo_switch))
            bs_proc.append("in_corso", new BsonString(pc.in_corso_switch))
            bs_proc.append("data_inizio_processo", new BsonString((if(pc.data_inizio_processo_switch!="")pc.data_inizio_processo_switch.replace("-","").substring(0,8) else "")))
            bs_proc.append("data_fine_processo", new BsonString((if(pc.data_fine_processo_switch!="")pc.data_fine_processo_switch.replace("-","").substring(0,8) else "")))
            bs_proc.append("note", new BsonString(pc.note_switch))
            bs_proc.append("data_di_decorrenza", new BsonString((if(pc.data_inizio_validita_switch!="")pc.data_inizio_validita_switch.replace("-","").substring(0,8) else "")))


            arrProcessi.add(bs_proc)
            dictPdrProc += (pdr + "_" + pc.id_processo_switch -> "ok")
          }
        }
      }

      var cc=0

      var sq=Seq[BsonDocument]()
      for (bpdr <- dictPdrs.values) {
        if (!bpdr.isEmpty)
        {
          val arrfor = bpdr.getArray("forniture")
          cc=cc + arrfor.size()

          bsonArPdrs.add(bpdr)
          if(cc>=maxcc)
          {

            dc.append("pod", bsonArPdrs)
            sq=sq :+ dc.clone()

            bsonArPdrs.clear()

            dc = new BsonDocument()
            dc.append("anagrafica", anagrafica)
            dc.append("codice_fiscale", new BsonString(codfisc))
            dc.append("id", new BsonString(randomUUID().toString))

            cc=0
          }



        }
      }




      if(!bsonArPdrs.isEmpty){
        dc.append("pod", bsonArPdrs)
        sq=sq :+ dc.clone()
      }

      dictPdrs.empty
      dictPdrForn.empty
      dictPdrProc.empty


      //dc.append("pdr", bsonArPdrs)
      //dc

      sq




    }

    )


    log.info("Scrittura forniture gas aventi piu di 20000 pod/forniture")

    var cicla =true
    for(ind <- 0 to (numSplitting +1)){

      val rdd_tmp=rdd_docs.map(f =>{if(f.length>ind)(f(ind)) else null}).filter(f => f != null)
      if(!rdd_tmp.partitions.isEmpty)
        writeDocsToMongoDb(rdd_tmp, dbMongo, collectionMongo)
    }


  }

  def ExportMisureElettrico(dbMongo :String,collectionMongo:String): Unit ={





    if(mongocollaudo) {
      log.info(s"Impostazione collection ${dbMongo}.${collectionMongo} in modalità shard")
      val connString = connectionStringMongoCollaudo.replace("pdc", "mongoadmin") + "/admin"
      val mongoClient: MongoClient = new MongoClient(new MongoClientURI(connString))
      val db = mongoClient.getDatabase("admin");


      //comandi per creare la collection in shard
      val dbx = mongoClient.getDatabase(dbMongo);
      Thread.sleep(5000)

      dbx.createCollection(collectionMongo)

      val documentA = db.runCommand(new Document("shardcollection", s"${dbMongo}.${collectionMongo}").append("key", new Document("_id", 1)))


        log.info(s"Esecuzione split sulla collection ${dbMongo}.${collectionMongo}")
       // val tmp = if (process_last_3_month) "3M" else "33M"
        val tmp =  "3M"
        val vals = hiveCtx.sql(s"select distinct chiave from misure.elenco_splits where collection='MisureElettriche${tmp}'").collect()

        println(s"select distinct chiave from misure.elenco_splits where collection='MisureElettriche${tmp}'")

        val cc=vals.length
        var ind=0
        var passo=5
        for (r <- vals) {
          db.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
            .append("middle", new Document("_id", r(0).toString)))
          //log.info(s"Valore di split : ${r(0).toString}")
          ind=ind+1
          val x=((ind*1.0)/(1.0* cc))*(100.0)
          if(x >=passo){
            println(s"Split inseriti : ${x}%" )
            passo=passo + 5
          }
          Thread.sleep(100)
        }
        println(s"Split inseriti : 100%" )

        if (mongoClient.isLocked)
          mongoClient.unlock()

        mongoClient.close()

      log.info(s"Esecuzione split sulla collection ${dbMongo}.${collectionMongo} terminato")

        /*for(r <- vals)
      {
        val mongoClientA: MongoClient = new MongoClient(new MongoClientURI(connString))
        val dbA = mongoClientA.getDatabase("admin");

        var commandfail=true
        while(commandfail) {
          try {
            log.info(s"Valore di split : ${r(0).toString}")
            dbA.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
              .append("middle", new Document("_id", r(0).toString)))

            commandfail=false
          }
          catch {
            case e: Exception => {Thread.sleep(10000)}
          }
        }

        if(mongoClientA.isLocked)
          mongoClientA.unlock()

        mongoClientA.close()



          var cicla=true
          var numcicli=0
          //al massimo attendo 50 minuti per verificare il bilanciamento
          while(cicla){
            val mongoClientA2: MongoClient = new MongoClient(new MongoClientURI(connString))
            val dbA2 = mongoClientA2.getDatabase("admin");
            val dbS = mongoClientA2.getDB(dbMongo)
            val c = dbS.getCollection(collectionMongo)

            val rtv =getShardDistribution(c)
            numcicli=numcicli+1
            if(rtv == -1 || rtv<=25 || numcicli>50)
              cicla=false
            else
              Thread.sleep(15000)

            if(mongoClientA2.isLocked)
              mongoClientA2.unlock()

            mongoClientA2.close()

          }



        //log.info(s"Valore di split : ${r(0).toString}")
      }*/



    }






    val timeZone = prop.getProperty("spark.app.time_zone")
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))


    val whereSplit=if(process_splitted_mis)
    {
      cal.add(Calendar.DAY_OF_YEAR, -90)
      val anno: String = Integer.toString(cal.get(Calendar.YEAR))
      val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
      val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
      val x= if(process_last_3_month)
          s" WHERE competenza_consumi > ${annomese}"
      else
          s" WHERE competenza_consumi <= ${annomese}"

      log.info(s"Estrazione misure elettriche con il seguente filtro : ${x}")
      x
    }else {
      log.info("Estrazione misure elettriche")
      " "
    }

    val dt = hiveCtx.sql(
      s"""
        SELECT fornitura_pod,misura_oraria_gg,misura_oraria_mese,misura_non_oraria,volture,autolettura
        ,competenza_consumi, giorno,pod
        FROM (
        select CONCAT(n_id_fornitura,'/b',pod)fornitura_pod ,CONCAT(nvl(competenza_consumi,'')  ,'/b',nvl(consumo_giornaliero_gg,'') ,'/b',
        nvl(lettura_giornaliero_f1,'') ,'/b',nvl(lettura_giornaliero_f2,'') ,'/b',
        nvl(lettura_giornaliero_f3,'') ,'/b',nvl(lettura_giornaliero_f4,'') ,'/b',nvl(lettura_giornaliero_f5,'') ,'/b',
        nvl(lettura_giornaliero_f6,'') ,'/b',nvl(delta_misure_f1,'') ,'/b', nvl(delta_misure_f2,'') ,'/b',
        nvl(delta_misure_f3,'') ,'/b',nvl(delta_misure_f4,'') ,'/b',nvl(delta_misure_f5,'') ,'/b',
        nvl(delta_misure_f6,'') ,'/b',nvl(giorno,''),'/b',nvl(potenza_max_erogata,''),'/b',nvl(tipo_flusso,''),'/b',nvl(data_lettura,'')) misura_oraria_gg,'' misura_oraria_mese,'' misura_non_oraria,'' volture,'' autolettura
        ,competenza_consumi,giorno,pod
        from  misure.misure_orarie_c${suffix_m} ${whereSplit}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',pod)fornitura_pod  ,'' misura_oraria_gg,
        CONCAT(nvl(competenza_consumi,'') ,'/b',nvl(delta_misura_monoraria,'') ,'/b',nvl(lettura_misura_monoraria,''),'/b',
        nvl(lettura_misura_f1,''),'/b',nvl(lettura_misura_f2,''),'/b',
        nvl(lettura_misura_f3,''),'/b',nvl(lettura_misura_f4,''),'/b',nvl(lettura_misura_f5,''),'/b',
        nvl(lettura_misura_f6,''),'/b',nvl(delta_misure_f1,''),'/b',nvl(delta_misure_f2,''),'/b',
        nvl(delta_misure_f3,''),'/b',nvl(delta_misure_f4,''),'/b',nvl(delta_misure_f5,''),'/b',
        nvl(delta_misure_f6,''),'/b',nvl(tipo_flusso,''),'/b',nvl(data_lettura,'')) misura_oraria_mese,'' misura_non_oraria,'' volture,''autolettura
        ,competenza_consumi,1 giorno,pod
        from misure.misure_mensili_c${suffix_m} ${whereSplit}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',pod)fornitura_pod ,'' misura_oraria_gg, ''misura_oraria_mese,
        CONCAT(nvl(competenza_consumi,'') ,'/b',nvl(delta_misura_monoraria,'') ,'/b',
        nvl(lettura_misura_monoraria,'') ,'/b',nvl(lettura_misura_f1,'') ,'/b',nvl(lettura_misura_f2,'') ,'/b',
        nvl(lettura_misura_f3,'') ,'/b',nvl(lettura_misura_f4,'') ,'/b',nvl(lettura_misura_f5,'') ,'/b',
        nvl(lettura_misura_f6,'') ,'/b',nvl(delta_misure_f1,'') ,'/b',nvl(delta_misure_f2,'') ,'/b',
        nvl(delta_misure_f3,'') ,'/b',nvl(delta_misure_f4,'') ,'/b',nvl(delta_misure_f5,'') ,'/b',
        nvl(delta_misure_f6,'') ,'/b',nvl(tipo_flusso2,' '),'/b',nvl(data_lettura,''),'/b',
        nvl(potf1,''),'/b',nvl(potf2,''),'/b',nvl(potf3,''),'/b',nvl(potm,'')) misura_non_oraria,'' volture,''autolettura
        ,competenza_consumi,1 giorno,pod
        from misure.misure_non_orarie_c${suffix_m} ${whereSplit}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',pod)fornitura_pod ,'' misura_oraria_gg, '' misura_oraria_mese,
        '' misura_non_oraria,CONCAT(nvl(competenza_consumi,'') ,'/b',nvl(data_lettura,'') ,'/b',
        nvl(lettura_misura_monoraria,'') ,'/b',nvl(lettura_misura_f1,'') ,'/b',nvl(lettura_misura_f2,'') ,'/b',
        nvl(lettura_misura_f3,'') ,'/b',nvl(lettura_misura_f4,'') ,'/b',nvl(lettura_misura_f5,'') ,'/b',
        nvl(lettura_misura_f6,''),'/b',nvl(tipo_flusso2,' ')) volture,''  autolettura,competenza_consumi,1 giorno,pod
        from misure.volture${suffix_m} ${whereSplit}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',pod)fornitura_pod ,'' misura_oraria_gg, '' misura_oraria_mese,
        '' misura_non_oraria,'' volture,CONCAT(nvl(competenza_consumi,'') ,'/b',nvl(data_lettura,'') ,'/b',
        nvl(lettura_misura_monoraria,'') ,'/b',nvl(lettura_misura_f1,'') ,'/b',nvl(lettura_misura_f2,'') ,'/b',
        nvl(lettura_misura_f3,'') ,'/b',nvl(lettura_misura_f4,'') ,'/b',nvl(lettura_misura_f5,'') ,'/b',
        nvl(lettura_misura_f6,''))  autolettura,competenza_consumi,1 giorno,pod
        from misure.autoletture${suffix_m} ${whereSplit}
        ) AS TBL  ORDER BY pod,competenza_consumi, giorno
       """)


    val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString))
    val rdd = tp.groupByKey().map { case (x, y) => (x, (y.toList)) }


    val rdd_docs = rdd.map( x => {
      val dc = new BsonDocument()

      val cod_fornitura_pod = x._1.split("/b",-1)
      val list = x._2

      dc.append("_id",new BsonString(cod_fornitura_pod(0)))
      dc.append("codice_fornitura",new BsonString(cod_fornitura_pod(0)))
      dc.append("pod",new BsonString(cod_fornitura_pod(1)))

      val misure = new BsonDocument()

      val bsonArAutoLetture = new BsonArray()
      val bsonArVolture = new BsonArray()
      val bsonArMisureOrarieGG = new BsonArray()
      val bsonArMisureOrarieMS = new BsonArray()
      val bsonArMisureNoOrarie = new BsonArray()

      var dictMisureGG: Map[String, String] = Map("" -> "")
      var dictMisureMese: Map[String, String] = Map("" -> "")
      var dictMisureAuto: Map[String, String] = Map("" -> "")
      var dictMisureNoOra: Map[String, String] = Map("" -> "")
      var dictMisureVolture: Map[String, String] = Map("" -> "")

      for (i <- list) {
        val misure_orarie_gg = i._1
        val misure_orarie_ms = i._2
        val misure_non_orarie_ms = i._3
        val misure_volture = i._4
        val misure_autolettura = i._5

        if(misure_autolettura!="")
          {

            val auto= misure_autolettura.split("/b",-1)

            val fn_auto = Misure_AutoLettura_Ele(auto(0),auto(1),auto(2),auto(3),auto(4),
              auto(5),auto(6),auto(7),auto(8))

            if (!dictMisureAuto.contains(fn_auto.competenza_consumi+"_auto")) {

              val bs_auto = new BsonDocument()
              bs_auto.append("competenza_consumi", new BsonString(fn_auto.competenza_consumi))
              bs_auto.append("data_lettura", getBsonValue(fn_auto.data_lettura))
              bs_auto.append("lettura_misura_monoraria", getBsonValue(fn_auto.lettura_misura_monoraria))
              bs_auto.append("lettura_misura_f1", getBsonValue(fn_auto.lettura_misura_f1))
              bs_auto.append("lettura_misura_f2", getBsonValue(fn_auto.lettura_misura_f2))
              bs_auto.append("lettura_misura_f3", getBsonValue(fn_auto.lettura_misura_f3))
              bs_auto.append("lettura_misura_f4", getBsonValue(fn_auto.lettura_misura_f4))
              bs_auto.append("lettura_misura_f5", getBsonValue(fn_auto.lettura_misura_f5))
              bs_auto.append("lettura_misura_f6", getBsonValue(fn_auto.lettura_misura_f6))
              bsonArAutoLetture.add(bs_auto)
              dictMisureAuto += (fn_auto.competenza_consumi+"_auto" -> "ok")
            }
          }
        if(misure_volture!="")
        {

          val volt= misure_volture.split("/b",-1)

          val fn_volt = Misure_Volture_Ele(volt(0),volt(1),volt(2),volt(3),volt(4),
            volt(5),volt(6),volt(7),volt(8),volt(9))

          if (!dictMisureVolture.contains(fn_volt.competenza_consumi+"_auto")) {

            val bs_volt = new BsonDocument()
            bs_volt.append("competenza_consumi", new BsonString(fn_volt.competenza_consumi))
            bs_volt.append("data_lettura", getBsonValue(fn_volt.data_lettura))
            bs_volt.append("lettura_misura_monoraria", getBsonValue(fn_volt.lettura_misura_monoraria))
            bs_volt.append("lettura_misura_f1", getBsonValue(fn_volt.lettura_misura_f1))
            bs_volt.append("lettura_misura_f2", getBsonValue(fn_volt.lettura_misura_f2))
            bs_volt.append("lettura_misura_f3", getBsonValue(fn_volt.lettura_misura_f3))
            bs_volt.append("lettura_misura_f4", getBsonValue(fn_volt.lettura_misura_f4))
            bs_volt.append("lettura_misura_f5", getBsonValue(fn_volt.lettura_misura_f5))
            bs_volt.append("lettura_misura_f6", getBsonValue(fn_volt.lettura_misura_f6))
            bs_volt.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_volt.tipo_misura)))
            bsonArVolture.add(bs_volt)
            dictMisureVolture += (fn_volt.competenza_consumi+"_volt" -> "ok")
          }
        }
        if(misure_orarie_gg!="")
        {

          val gg= misure_orarie_gg.split("/b",-1)

          val fn_gg = Misure_Orarie_GG_Ele(gg(0),gg(1),gg(2),gg(3),gg(4),
            gg(5),gg(6),gg(7),gg(8),gg(9),gg(10),gg(11),gg(12),gg(13),gg(14),gg(15),gg(16),gg(17))

          if (!dictMisureGG.contains(fn_gg.competenza_consumi+"_"+fn_gg.giorno+"_gg")) {

            val bs_gg = new BsonDocument()
            bs_gg.append("competenza_consumi", new BsonString(fn_gg.competenza_consumi))
            bs_gg.append("consumo_giornaliero_gg", new BsonString(fn_gg.consumo_giornaliero_gg))
            bs_gg.append("lettura_misura_f1", new BsonString(fn_gg.lettura_giornaliero_f1))
            bs_gg.append("lettura_misura_f2", new BsonString(fn_gg.lettura_giornaliero_f2))
            bs_gg.append("lettura_misura_f3", new BsonString(fn_gg.lettura_giornaliero_f3))
            bs_gg.append("lettura_misura_f4", new BsonString(fn_gg.lettura_giornaliero_f4))
            bs_gg.append("lettura_misura_f5", new BsonString(fn_gg.lettura_giornaliero_f5))
            bs_gg.append("lettura_misura_f6", new BsonString(fn_gg.lettura_giornaliero_f6))
            bs_gg.append("delta_misure_f1", new BsonString(fn_gg.delta_misure_f1))
            bs_gg.append("delta_misure_f2", new BsonString(fn_gg.delta_misure_f2))
            bs_gg.append("delta_misure_f3", new BsonString(fn_gg.delta_misure_f3))
            bs_gg.append("delta_misure_f4", new BsonString(fn_gg.delta_misure_f4))
            bs_gg.append("delta_misure_f5", new BsonString(fn_gg.delta_misure_f5))
            bs_gg.append("delta_misure_f6", new BsonString(fn_gg.delta_misure_f6))
            bs_gg.append("giorno", new BsonString(fn_gg.competenza_consumi+(("0" + fn_gg.giorno) takeRight 2)))
            bs_gg.append("potenza_max_erogata", new BsonString(fn_gg.potenza_max_erogata))
            bs_gg.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_gg.tipo_misura)))
            bs_gg.append("data_lettura", new BsonString(fn_gg.data_lettura))

            bsonArMisureOrarieGG.add(bs_gg)
            dictMisureGG += (fn_gg.competenza_consumi+"_"+fn_gg.giorno+"_gg" -> "ok")
          }
        }

        if(misure_orarie_ms!="" || misure_non_orarie_ms!="")
        {


          if(misure_orarie_ms!="") {
            val mo_ms = misure_orarie_ms.split("/b", -1)

            val fn_ms = Misure_Orarie_MS_Ele(mo_ms(0), mo_ms(1), mo_ms(2), mo_ms(3), mo_ms(4), mo_ms(5)
              , mo_ms(6), mo_ms(7), mo_ms(8), mo_ms(9), mo_ms(10), mo_ms(11), mo_ms(12), mo_ms(13), mo_ms(14)
              , mo_ms(15),mo_ms(16),"","","","")




            if (!dictMisureMese.contains(fn_ms.competenza_consumi + "_mso")) {

              val bs_ms = new BsonDocument()
              bs_ms.append("competenza_consumi", new BsonString(fn_ms.competenza_consumi))
              bs_ms.append("lettura_misura_monoraria", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_monoraria)))
              bs_ms.append("lettura_misura_f1", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f1)))
              bs_ms.append("lettura_misura_f2", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f2)))
              bs_ms.append("lettura_misura_f3", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f3)))
              bs_ms.append("lettura_misura_f4", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f4)))
              bs_ms.append("lettura_misura_f5", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f5)))
              bs_ms.append("lettura_misura_f6", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.lettura_misura_f6)))
              bs_ms.append("delta_misure_f1", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f1)))
              bs_ms.append("delta_misure_f2", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f2)))
              bs_ms.append("delta_misure_f3", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f3)))
              bs_ms.append("delta_misure_f4", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f4)))
              bs_ms.append("delta_misure_f5", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f5)))
              bs_ms.append("delta_misure_f6", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misure_f6)))
              bs_ms.append("delta_misure_monoraria", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura,fn_ms.delta_misura_monoraria)))
              bs_ms.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_ms.tipo_misura)))
              bs_ms.append("data_lettura", getBsonValue(fn_ms.data_lettura))
              bs_ms.append("potf1", getBsonValue(fn_ms.potf1))
              bs_ms.append("potf2", getBsonValue(fn_ms.potf2))
              bs_ms.append("potf3", getBsonValue(fn_ms.potf3))
              bs_ms.append("potm", getBsonValue(fn_ms.potm))

              bsonArMisureOrarieMS.add(bs_ms)
              dictMisureMese += (fn_ms.competenza_consumi + "_mso" -> "ok")
            }
          }

          if(misure_non_orarie_ms!="") {
            val mo_ms_nora = misure_non_orarie_ms.split("/b", -1)

            val fno_ms = Misure_NonOrarie_Ele(mo_ms_nora(0), mo_ms_nora(1), mo_ms_nora(2), mo_ms_nora(3), mo_ms_nora(4), mo_ms_nora(5)
              , mo_ms_nora(6), mo_ms_nora(7), mo_ms_nora(8), mo_ms_nora(9), mo_ms_nora(10), mo_ms_nora(11), mo_ms_nora(12), mo_ms_nora(13), mo_ms_nora(14)
              , mo_ms_nora(15),mo_ms_nora(16),mo_ms_nora(17),mo_ms_nora(18),mo_ms_nora(19),mo_ms_nora(20))

            val insMisMens= (!dictMisureMese.contains(fno_ms.competenza_consumi + "_mso"))
            val insMisNonOra= (!dictMisureNoOra.contains(fno_ms.competenza_consumi+"_msno"))

            if(insMisMens || insMisNonOra) {
              val bs_ms = new BsonDocument()
              bs_ms.append("competenza_consumi", new BsonString(fno_ms.competenza_consumi))
              bs_ms.append("lettura_misura_monoraria", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_monoraria)))
              bs_ms.append("lettura_misura_f1", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f1)))
              bs_ms.append("lettura_misura_f2", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f2)))
              bs_ms.append("lettura_misura_f3", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f3)))
              bs_ms.append("lettura_misura_f4", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f4)))
              bs_ms.append("lettura_misura_f5", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f5)))
              bs_ms.append("lettura_misura_f6", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.lettura_misura_f6)))
              bs_ms.append("delta_misure_monoraria", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misura_monoraria)))
              bs_ms.append("delta_misure_f1", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f1)))
              bs_ms.append("delta_misure_f2", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f2)))
              bs_ms.append("delta_misure_f3", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f3)))
              bs_ms.append("delta_misure_f4", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f4)))
              bs_ms.append("delta_misure_f5", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f5)))
              bs_ms.append("delta_misure_f6", getBsonValue(setMisuraByTipoMisura(fno_ms.tipo_misura,fno_ms.delta_misure_f6)))
              bs_ms.append("tipo_misura", new BsonString(getDescrTipoMisura(fno_ms.tipo_misura)))
              bs_ms.append("data_lettura", getBsonValue(fno_ms.data_lettura))
              bs_ms.append("potf1", getBsonValue(fno_ms.potf1))
              bs_ms.append("potf2", getBsonValue(fno_ms.potf2))
              bs_ms.append("potf3", getBsonValue(fno_ms.potf3))
              bs_ms.append("potm", getBsonValue(fno_ms.potm))

              if (insMisMens) {

                bsonArMisureOrarieMS.add(bs_ms)
                dictMisureMese += (fno_ms.competenza_consumi + "_mso" -> "ok")

              }
              if (insMisNonOra) {

                bsonArMisureNoOrarie.add(bs_ms.clone())
                dictMisureNoOra += (fno_ms.competenza_consumi + "_msno" -> "ok")
              }
            }


          }

        }




      }


      if(!bsonArAutoLetture.isEmpty)
      misure.append("autoletture",bsonArAutoLetture)
      if(!bsonArVolture.isEmpty)
       misure.append("volture",bsonArVolture)
      if(!bsonArMisureOrarieGG.isEmpty)
      misure.append("misure_orarie",bsonArMisureOrarieGG)
      if(!bsonArMisureOrarieMS.isEmpty)
      misure.append("misure_mensili",bsonArMisureOrarieMS)
      if(!bsonArMisureNoOrarie.isEmpty)
      misure.append("misure_non_orarie",bsonArMisureNoOrarie)


      dictMisureAuto.empty
      dictMisureVolture.empty
      dictMisureGG.empty
      dictMisureMese.empty
      dictMisureNoOra.empty

      dc.append("misure",misure)
      dc
    }

    )


    log.info("Scrittura misure elettrico")



    //istruzioni per droppare la collection
    if(!mongocollaudo) {
      try {
        val dt_clean = MongoSpark.read(hiveCtx).load().filter("codice_fornitura = ' '")
        MongoSpark.save(dt_clean.write.mode("overwrite"))
      } catch {
        case e: Exception => {}
      }
    }

    //per la versione 4.4. bisogna fare una seconda drop


   /* if(!mongocollaudo) {
      try {
        val data = MongoSpark.load(hiveCtx, ReadConfig(Map("collection" -> "TestMisureEE"), Some(ReadConfig(hiveCtx))))
        MongoSpark.save(data.write.mode(SaveMode.Append))
      } catch {
        case e: Exception => {}
      }
    }*/


      if (mongocollaudo) {
        val connString = connectionStringMongoCollaudo.replace("pdc", "mongoadmin") + "/admin"
        val mongoClient = new MongoClient(new MongoClientURI(connString))
        val db = mongoClient.getDB(dbMongo)
        val c = db.getCollection(collectionMongo)
        var cicla = true
        var numcicli = 0
        //al massimo attendo 50 minuti per verificare il bilanciamento
        while (cicla) {
          val rtv = getShardDistribution(c)
          numcicli = numcicli + 1
          if (rtv == -1 || rtv <= 15 || numcicli > 50)
            cicla = false
          else
            Thread.sleep(60000)
        }
      }

        val writeConfig = WriteConfig(Map("uri"->mongouri, "database" -> dbMongo,"collection" -> collectionMongo,"replicaSet"->"rset","writeConcern.w"->writeConcern_Write_Prod,"writeConcern.j"->writeConcern_Journal_Prod))
        /*if(!mongocollaudo)MongoSpark.save(rdd_docs, writeConfig)
        else*/ saveUnordered(rdd_docs,writeConfig)


  }



   def ExportMisureGas(dbMongo:String,collectionMongo:String): Unit = {

     if (mongocollaudo) {

       log.info(s"Impostazione collection ${dbMongo}.${collectionMongo} in modalità shard")
       val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
       val mongoClient: MongoClient = new MongoClient(new MongoClientURI(connString))
       val db = mongoClient.getDatabase("admin");


       //comandi per creare la collection in shard
       val dbx = mongoClient.getDatabase(dbMongo);
      /* val coll_old = dbx.getCollection(collectionMongo);
       coll_old.drop()
       coll_old.drop()
       Thread.sleep(5000)*/

       dbx.createCollection(collectionMongo)

       val documentA = db.runCommand(new Document("shardcollection", s"${dbMongo}.${collectionMongo}").append("key", new Document("_id", 1)))

       log.info(s"Esecuzione split sulla collection ${dbMongo}.${collectionMongo}")
       val tmp =if(process_last_3_month)"3M" else "33M"
       val vals = hiveCtx.sql(s"select distinct chiave from misure.elenco_splits where collection='MisureGas${tmp}'").collect()

       val cc=vals.length
       var ind=0
       var passo=5
       for (r <- vals) {
         db.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
           .append("middle", new Document("_id", r(0).toString)))
         //log.info(s"Valore di split : ${r(0).toString}")
         ind=ind+1
         val x=((ind*1.0)/(1.0* cc))*(100.0)
         if(x >=passo){
           println(s"Split inseriti : ${x}%" )
           passo=passo + 5
         }
         Thread.sleep(100)
       }
       println(s"Split inseriti : 100%" )

       if(mongoClient.isLocked)
         mongoClient.unlock()

       mongoClient.close()


       /* val dbS = mongoClient.getDB(dbMongo)
        val c = dbS.getCollection(collectionMongo)

        for(r <- vals)
        {

          var commandfail=true
          while(commandfail) {
            try {
              log.info(s"Valore di split : ${r(0).toString}")
              db.runCommand(new Document("split", s"${dbMongo}.${collectionMongo}")
                .append("middle", new Document("_id", r(0).toString)))

              commandfail=false
            }
            catch {
              case e: Exception => {Thread.sleep(10000)}
            }
          }
          var cicla=true
          var numcicli=0
          //al massimo attendo 50 minuti per verificare il bilanciamento
          while(cicla){
            val rtv =getShardDistribution(c)
            numcicli=numcicli+1
            if(rtv == -1 || rtv<=25 || numcicli>50)
              cicla=false
            else
              Thread.sleep(15000)
          }

          //log.info(s"Valore di split : ${r(0).toString}")
        }*/

       println(s"select distinct chiave from misure.elenco_splits where collection='MisureGas${tmp}'")
       Thread.sleep(60000)
     }

     val timeZone = prop.getProperty("spark.app.time_zone")
     val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))

     val whereSplit: String = if (process_splitted_mis) {
       cal.add(Calendar.DAY_OF_YEAR, -90)
       val anno: String = Integer.toString(cal.get(Calendar.YEAR))
       val mese: String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
       val annomese = anno.toString + (("0" + mese.toString) takeRight 2)
       val x = if (process_last_3_month)
         s" WHERE competenza_consumi > ${annomese}"
       else
         s" WHERE competenza_consumi <= ${annomese}"

       log.info(s"Estrazione misure gas con il seguente filtro : ${x}")
       x
     } else {
       log.info("Estrazione misure gas")
       ""
     }

     val dt = hiveCtx.sql(
       s"""
        SELECT fornitura_pdr,misura_gg,misura_mese_af,autolettura,misure_mese,volture
         ,competenza_consumi, giorno,cod_pdr FROM (
        select CONCAT(n_id_fornitura,'/b',cod_pdr)fornitura_pdr ,'' misura_gg,
        CONCAT(nvl(competenza_consumi_af,'') ,'/b', nvl(data_lettura_af,'') ,'/b',nvl(lettura_mese_af,''),'/b',
        nvl(delta_misure_af,''),'/b',nvl(tipo_misura_af,'')) misura_mese_af,
        '' autolettura,'' misure_mese,'' volture,
        competenza_consumi_af competenza_consumi,1 giorno,cod_pdr
        from ${dbgas}.misure_gas_portale_af${suffix_m} ${(whereSplit).replace("competenza_consumi", "competenza_consumi_af")}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',cod_pdr)fornitura_pdr ,
        CONCAT(nvl(competenza_consumi_gg,'') ,'/b', nvl(data_lettura_gg,'') ,'/b', nvl(lettura_gg,'') ,'/b',
         nvl(delta_misure_gg,''),'/b',nvl(tipo_misura_gg,'')) misura_gg, '' misura_mese_af,
        ''  autolettura ,'' misure_mese,'' volture,
        competenza_consumi_gg competenza_consumi,data_lettura_gg giorno,cod_pdr
        from  ${dbgas}.misure_gas_portale_gg${suffix_m} ${(whereSplit).replace("competenza_consumi", "competenza_consumi_gg")}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',cod_pdr)fornitura_pdr ,'' misura_gg,
        '' misura_mese_af,CONCAT(nvl(competenza_consumi_autoletture,'') ,'/b',nvl(data_lettura_autoletture,'') ,'/b',
        nvl(lettura_mese_autoletture,'') ,'/b',nvl(dt_caricamento,''),'/b',nvl(tipo_lettura_autoletture,''))  autolettura,
        '' misure_mese ,'' volture ,
        competenza_consumi_autoletture competenza_consumi,1 giorno,cod_pdr
        from ${dbgas}.consumi_autoletture${suffix_m} ${(whereSplit).replace("competenza_consumi", "competenza_consumi_autoletture")}
        UNION ALL
        select CONCAT(n_id_fornitura,'/b',cod_pdr)fornitura_pdr ,
        '' misura_gg, '' misura_mese_af, ''  autolettura ,
         CONCAT(nvl(competenza_consumi_mmmm,'') ,'/b', nvl(data_lettura_mmmm,'') ,'/b',nvl(lettura_misure_mmmm,''),'/b',
        nvl(delta_misure_mmmm,''),'/b',nvl(tipo_misura_mmmm,''))misure_mese ,'' volture,
        competenza_consumi_mmmm competenza_consumi,1 giorno,cod_pdr
        from  ${dbgas}.misure_gas_portale_mensili${suffix_m} ${(whereSplit).replace("competenza_consumi", "competenza_consumi_mmmm")}
        UNION ALL
        select CONCAT(codice_fornitura_v,'/b',cod_pdr)fornitura_pdr ,'' misura_gg,
        '' misura_mese_af, '' autolettura,
        '' misure_mese ,CONCAT(nvl(competenza_consumi_v,'') ,'/b',nvl(data_lettura_v,'') ,'/b',
         nvl(lettura_v,'') ,'/b',nvl(tipo_misure_v,'')) volture ,
        competenza_consumi_v competenza_consumi,1 giorno,cod_pdr
        from ${dbgas}.volturegas${suffix_m} ${(whereSplit).replace("competenza_consumi", "competenza_consumi_v")}
        ) AS TBL  ORDER BY cod_pdr,competenza_consumi,giorno
       """)


     val tp = dt.map(row => row(0).toString -> (row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString))
     val rdd = tp.groupByKey().map { case (x, y) => (x, (y.toList)) }


     val rdd_docs = rdd.map(x => {
       val dc = new BsonDocument()

       val cod_fornitura_pod = x._1.split("/b", -1)
       val list = x._2

       dc.append("_id", new BsonString(cod_fornitura_pod(0)))
       dc.append("codice_fornitura", new BsonString(cod_fornitura_pod(0)))
       dc.append("pdr", new BsonString(cod_fornitura_pod(1)))

       val misure = new BsonDocument()

       val bsonArAutoLetture = new BsonArray()
       val bsonArVolture = new BsonArray()
       val bsonArMisureGG = new BsonArray()
       val bsonArMisureMMAF = new BsonArray()
       val bsonArMisureMM = new BsonArray()

       var dictMisure: Map[String, String] = Map("" -> "")


       for (i <- list) {
         val misure_gg = i._1
         val misure_ms_af = i._2
         val misure_autolettura = i._3
         val misure_ms = i._4
         val misure_volture = i._5

         if (misure_autolettura != "") {

           val auto = misure_autolettura.split("/b", -1)

           val fn_auto = Misure_AutoLettura_GAS(auto(0), auto(1), auto(2), auto(3), auto(4))


           if (!dictMisure.contains(fn_auto.competenza_consumi + "_auto")) {

             val bs_auto = new BsonDocument()
             bs_auto.append("competenza_consumi", new BsonString(fn_auto.competenza_consumi))
             bs_auto.append("data_lettura", getBsonValue((if (fn_auto.data_lettura.length > 8) fn_auto.data_lettura.replace("-", "").substring(0, 8) else "")))
             bs_auto.append("lettura_mese", getBsonValue(fn_auto.lettura_misura_mese))
             //bs_auto.append("data_caricamento", getBsonValue((if(fn_auto.dt_caricamento.length>8)fn_auto.dt_caricamento.replace("-","").substring(0,8) else "")))
             bs_auto.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_auto.tipo_lettura)))

             bsonArAutoLetture.add(bs_auto)
             dictMisure += (fn_auto.competenza_consumi + "_auto" -> "ok")
           }
         }
         if (misure_gg != "") {

           val gg = misure_gg.split("/b", -1)

           val fn_gg = Misure_GG_GAS(gg(0), gg(1), gg(2), gg(3), gg(4))

           if (!dictMisure.contains(fn_gg.competenza_consumi + "_" + fn_gg.data_lettura_giornaliero + "_gg")) {

             val bs_gg = new BsonDocument()
             bs_gg.append("competenza_consumi", new BsonString(fn_gg.competenza_consumi))
             bs_gg.append("data_lettura", new BsonString((if (fn_gg.data_lettura_giornaliero.length > 8) fn_gg.data_lettura_giornaliero.replace("-", "").substring(0, 8) else "")))
             bs_gg.append("delta_misure", new BsonString(fn_gg.delta_misure_giornaliero))
             bs_gg.append("lettura_giorno", new BsonString(fn_gg.lettura_giornaliero))
             bs_gg.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_gg.tipo_misura)))


             bsonArMisureGG.add(bs_gg)
             dictMisure += (fn_gg.competenza_consumi + "_" + fn_gg.data_lettura_giornaliero + "_gg" -> "ok")
           }
         }

         if (misure_ms_af != "") {


           val mo_ms_af = misure_ms_af.split("/b", -1)

           val fn_ms_af = Misure_MS_GAS(mo_ms_af(0), mo_ms_af(1), mo_ms_af(2), mo_ms_af(3), mo_ms_af(4))

           if (!dictMisure.contains(fn_ms_af.competenza_consumi + "_msaf")) {

             val bs_ms = new BsonDocument()
             bs_ms.append("competenza_consumi", new BsonString(fn_ms_af.competenza_consumi))
             bs_ms.append("data_lettura", getBsonValue((if (fn_ms_af.data_lettura_ms.length > 8) fn_ms_af.data_lettura_ms.replace("-", "").substring(0, 8) else "")))
             bs_ms.append("delta_misure", getBsonValue(setMisuraByTipoMisura(fn_ms_af.tipo_misura_ms, fn_ms_af.delta_misure_ms)))
             bs_ms.append("lettura_mese", getBsonValue(setMisuraByTipoMisura(fn_ms_af.tipo_misura_ms, fn_ms_af.lettura_mese)))
             bs_ms.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_ms_af.tipo_misura_ms)))

             bsonArMisureMMAF.add(bs_ms)
             dictMisure += (fn_ms_af.competenza_consumi + "_msaf" -> "ok")
           }
         }

         if (misure_ms != "") {

           val mo_ms = misure_ms.split("/b", -1)

           val fn_ms = Misure_MS_GAS(mo_ms(0), mo_ms(1), mo_ms(2), mo_ms(3), mo_ms(4))

           if (!dictMisure.contains(fn_ms.competenza_consumi + "_ms")) {

             val bs_ms = new BsonDocument()
             bs_ms.append("competenza_consumi", new BsonString(fn_ms.competenza_consumi))
             bs_ms.append("data_lettura", getBsonValue((if (fn_ms.data_lettura_ms.length > 8) fn_ms.data_lettura_ms.replace("-", "").substring(0, 8) else "")))
             bs_ms.append("delta_misure", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura_ms, fn_ms.delta_misure_ms)))
             bs_ms.append("lettura_mese", getBsonValue(setMisuraByTipoMisura(fn_ms.tipo_misura_ms, fn_ms.lettura_mese)))
             bs_ms.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_ms.tipo_misura_ms)))

             bsonArMisureMM.add(bs_ms)
             dictMisure += (fn_ms.competenza_consumi + "_ms" -> "ok")
           }
         }

         if (misure_volture != "") {

           val volt = misure_volture.split("/b", -1)

           val fn_volt = Misure_Volture_GAS(volt(0), volt(1), volt(2), volt(3))

           if (!dictMisure.contains(fn_volt.competenza_consumi + "_volt")) {

             val bs_volt = new BsonDocument()
             bs_volt.append("competenza_consumi", new BsonString(fn_volt.competenza_consumi))
             bs_volt.append("data_lettura", getBsonValue((if (fn_volt.data_lettura.length > 8) fn_volt.data_lettura.replace("-", "").substring(0, 8) else "")))
             bs_volt.append("lettura_misura", getBsonValue(fn_volt.lettura_misura))
             bs_volt.append("tipo_misura", new BsonString(getDescrTipoMisura(fn_volt.tipo_lettura)))

             bsonArVolture.add(bs_volt)
             dictMisure += (fn_volt.competenza_consumi + "_volt" -> "ok")
           }
         }


       }


       if (!bsonArAutoLetture.isEmpty)
         misure.append("autoletture", bsonArAutoLetture)
       if (!bsonArMisureGG.isEmpty)
         misure.append("misure_giornaliere", bsonArMisureGG)
       if (!bsonArMisureMMAF.isEmpty)
         misure.append("misure_altre_frequenze", bsonArMisureMMAF)
       if (!bsonArMisureMM.isEmpty)
         misure.append("misure_mensili", bsonArMisureMM)
       if (!bsonArVolture.isEmpty)
         misure.append("volture", bsonArVolture)

       dictMisure.empty

       dc.append("misure", misure)
       dc
     }

     )


     log.info("Scrittura misure gas")


     //istruzioni per droppare la collection
     if (!mongocollaudo) {
       try {
         val dt_clean = MongoSpark.read(hiveCtx).load().filter("codice_fornitura = ' '")
         MongoSpark.save(dt_clean.write.mode("overwrite"))
       } catch {
         case e: Exception => {}
       }
     }

     //per la versione 4.4. bisogna fare una seconda drop

    /* val docTemplate: org.mongodb.scala.bson.Document = org.mongodb.scala.bson.Document("_id" -> 0,
       "codice_fornitura" -> "F0002", "misure" -> org.mongodb.scala.bson.BsonArray())
    */

    /*
       try {
         val data = MongoSpark.load(hiveCtx, ReadConfig(Map("collection" -> "TestMisureGas"), Some(ReadConfig(hiveCtx))))
         MongoSpark.save(data.write.mode(SaveMode.Append))
       } catch {
         case e: Exception => {}
       }
     */


     if(mongocollaudo)
     {
       val connString = connectionStringMongoCollaudo.replace("pdc","mongoadmin")+"/admin"
       val mongoClient = new MongoClient(new MongoClientURI(connString))
       val db = mongoClient.getDB(dbMongo)
       val c = db.getCollection(collectionMongo)
       var cicla=true
       var numcicli=0
       //al massimo attendo 50 minuti per verificare il bilanciamento
       while(cicla){
         val rtv =getShardDistribution(c)
         numcicli=numcicli+1
         if(rtv == -1 || rtv<=15 || numcicli>50)
           cicla=false
         else
           Thread.sleep(60000)
       }
     }
       val writeConfig = WriteConfig(Map("uri" -> mongouri, "database" -> dbMongo, "collection" -> collectionMongo, "replicaSet" -> "rset", "writeConcern.w" -> writeConcern_Write_Prod, "writeConcern.j" -> writeConcern_Journal_Prod))
       /*if(!mongocollaudo)MongoSpark.save(rdd_docs, writeConfig)
       else*/ saveUnordered(rdd_docs,writeConfig)

   }


  def getDescrTipoMisura(tp_misu:String): String ={

    val tp_mis = if(tp_misu.startsWith("SW_"))tp_misu.replaceAll("SW_","") else tp_misu

    val l_tipo_misure = List(List("PDO", "PNO", "PDO2G", "PNO2G","Lettura Periodica")
    ,List("VNO", "VNO2G","Lettura Voltura")
    ,List("RFO", "RFO2G", "RNO", "RNO2G","Lettura di Rettifica")
    ,List("RNV", "RNV2G","Lettura di Rettifica Voltura")
    ,List("TGL", "TML","Lettura Periodica")
    ,List("VTG6","Lettura Voltura")
    ,List("RGL", "RML","Lettura di Rettifica")
    ,List("RMV","Lettura di Rettifica Voltura")
    ,List("TAL", "TAV","Autoletture"))

    for (el <- l_tipo_misure)
      {
        val f = el.find(_.equals(tp_mis.toUpperCase))
        if(f.isDefined) return el(el.length-1)
      }
    return tp_mis
  }

  def setMisuraByTipoMisura(tipo_misura:String,misura:String):String = {
  if(tipo_misura.startsWith("SW_")) ""  else misura
  }

  def getBsonValue(strval:String): BsonValue ={
    if(strval.trim=="")BsonNull.VALUE
    else new BsonString(strval)
  }
}




case class Fornitura_Ele(cod_fornitura:String, data_inizio_fornitura:String, data_fine_fornitura:String, tipo_mercato:String,
                     residente:String, tariffa:String, tensione:String, potenza_disponibile:String,
                     potenza_impegnata:String, stato_misuratore_2g:String, toponimo:String, nome_strada:String,
                     civico:String, comune:String, cap:String, provincia:String,
                     nazione:String, trattamento:String)

case class Fornitura_Ele2(tipo_misuratore:String, matricola_misuratore:String, p_iva_cc:String,
                       ragione_sociale_cc:String, ragione_sociale_distributore:String)

case class Processo_Ele_Gas(data_inizio_processo_gdm:String, data_fine_processo_gdm:String,
                    data_inizio_validita_gdm:String, id_processo_gdm:String, in_corso_gdm:String, note_gdm:String,
                    tipo_processo_gdm:String, data_inizio_processo_switch:String, data_fine_processo_switch:String, data_inizio_validita_switch:String,
                    id_processo_switch:String, in_corso_switch:String, note_switch:String, tipo_processo_switch:String)


case class FasceForniture_Ele(f_lunedi:String,f_martedi:String,f_mercoledi:String,f_giovedi:String,f_venerdi:String,f_sabato:String,
                            f_domenica:String,f_festivo:String,d_inizio_validita_fascia:String,d_fine_validita_fascia:String,d_data_iniziofreezing:String,id_misuratore_fasce:String)


case class Misure_AutoLettura_Ele(competenza_consumi :String,data_lettura :String,
lettura_misura_monoraria :String,lettura_misura_f1 :String,lettura_misura_f2 :String,
lettura_misura_f3 :String,lettura_misura_f4 :String,lettura_misura_f5 :String, lettura_misura_f6:String)

case class Misure_Volture_Ele(competenza_consumi :String,data_lettura :String,
lettura_misura_monoraria :String,lettura_misura_f1 :String,lettura_misura_f2 :String,
lettura_misura_f3 :String,lettura_misura_f4 :String,lettura_misura_f5 :String, lettura_misura_f6:String,tipo_misura :String)

case class Misure_Orarie_GG_Ele(competenza_consumi  :String,consumo_giornaliero_gg:String,
lettura_giornaliero_f1 :String,lettura_giornaliero_f2 :String,
lettura_giornaliero_f3 :String,lettura_giornaliero_f4 :String,lettura_giornaliero_f5 :String,
lettura_giornaliero_f6 :String,delta_misure_f1 :String,delta_misure_f2 :String,
delta_misure_f3 :String,delta_misure_f4 :String,delta_misure_f5 :String,
delta_misure_f6 :String,giorno :String,potenza_max_erogata :String,tipo_misura :String,data_lettura :String)

case class Misure_Orarie_MS_Ele(competenza_consumi :String,delta_misura_monoraria:String,
                                lettura_misura_monoraria:String, lettura_misura_f1:String,lettura_misura_f2:String,
                                lettura_misura_f3:String,lettura_misura_f4:String,lettura_misura_f5:String,
                                lettura_misura_f6:String,delta_misure_f1:String,delta_misure_f2:String,
                                delta_misure_f3:String,delta_misure_f4:String,delta_misure_f5:String,
                                delta_misure_f6:String,tipo_misura :String,data_lettura :String,
                                potf1 :String,potf2 :String,potf3 :String,potm :String)


case class Misure_NonOrarie_Ele(competenza_consumi :String,delta_misura_monoraria:String,
                                lettura_misura_monoraria :String,lettura_misura_f1 :String,lettura_misura_f2 :String,
                                lettura_misura_f3 :String,lettura_misura_f4 :String,lettura_misura_f5 :String,
                                lettura_misura_f6 :String,delta_misure_f1 :String,delta_misure_f2 :String,
                                delta_misure_f3 :String,delta_misure_f4 :String,delta_misure_f5 :String,
                                delta_misure_f6 :String,tipo_misura :String,data_lettura :String,
                                potf1 :String,potf2 :String,potf3 :String,potm :String)


case class FornituraGas(t_cap:String, categoria_duso:String, civico:String, classe_misuratore:String,
                        codice_fornitura:String, coefficiente_conversione:String, t_comune:String, data_fine_fornitura:String,
                        data_inizio_fornitura:String, matricola_misuratore:String, nazione:String, nome_strada:String,
                        piva_cc:String, provincia:String, ragione_sociale_cc:String, ragione_sociale_distributore:String,
                        residente:String, tipo_fornitura:String, tipo_pdr:String, toponimo_indirizzo:String)

case class Misure_AutoLettura_GAS(competenza_consumi :String,data_lettura :String,
                                  lettura_misura_mese :String,dt_caricamento :String,tipo_lettura:String)

case class Misure_GG_GAS(competenza_consumi  :String,data_lettura_giornaliero:String,
                                lettura_giornaliero :String,delta_misure_giornaliero :String,tipo_misura :String)

case class Misure_MS_GAS(competenza_consumi  :String,data_lettura_ms:String,
                         lettura_mese :String,delta_misure_ms :String,tipo_misura_ms :String)

case class Misure_Volture_GAS(competenza_consumi :String,data_lettura :String,
                                  lettura_misura :String,tipo_lettura:String)




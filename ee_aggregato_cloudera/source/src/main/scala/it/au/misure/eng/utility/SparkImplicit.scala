package it.au.misure.eng.utility

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Serializer;


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.Charset


trait SparkImplicit {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: SQLContext = _
  @transient var _hiveContext: HiveContext = _


  implicit lazy val sc: SparkContext = _sc
  //implicit lazy val sqlContext: SQLContext = _sqlContext
  implicit lazy val hiveContext: HiveContext = _hiveContext


  def initializeSpark(appName: String): Unit = {
    var conf = new SparkConf()
      .setAppName(appName)
//      .registerKryoClasses(Array (classOf[MyRegistrator]))
//      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.kryo.registrator", "it.au.misure.eng.utility.MyRegistrator")



    if(SystemUtility.isLocalLaunch) {
      conf = conf.setMaster("local[*]")
    } else {
      conf = conf.setMaster("yarn-client")
    }

    _sc = SparkContext.getOrCreate(conf)

    _sqlContext = SQLContext.getOrCreate(_sc)

    _hiveContext = new HiveContext(_sc)
    _hiveContext.setConf("hive.exec.dynamic.partition", "true")
    _hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    _hiveContext.setConf("spark.sql.sources.partitionOverwriteMode", "dynamic")
  }
}

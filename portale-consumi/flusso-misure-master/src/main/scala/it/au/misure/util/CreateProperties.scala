package it.au.misure.util


/*
 * https://stackoverflow.com/questions/31115881/how-to-load-java-properties-file-and-use-in-spark
 * 
 * job.property -> app.name=xyz
 * 
 * spark-submit --properties-file  job.property ...
 * 
 * spark-submit --files job.properties ...
 * 
   import java.util.Properties;
   import org.apache.hadoop.fs.FSDataInputStream;
   import org.apache.hadoop.fs.FileSystem;
   import org.apache.hadoop.fs.Path;
   import org.apache.spark.SparkFiles;
   
   //Load file to propert object using HDFS FileSystem
   String fileName = SparkFiles.get("job.properties")
   Configuration hdfsConf = new Configuration();
   FileSystem fs = FileSystem.get(hdfsConf);
   
   //THe file name contains absolute path of file
   FSDataInputStream is = fs.open(new Path(fileName));
   Properties prop = new Properties();
   //load properties
   prop.load(is)
   //retrieve properties
   prop.getProperty("app.name");
 * 
 */

import java.util.Properties;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.hadoop.conf.Configuration;


/**
 * ==CreateProperties== 
 * Utilità per la lettura delle configurl jaazioni su file system hdfs.
 */
class CreateProperties {
  private var _cur_user =""
  private var _dir_started_driver:String =""
  // val envVar :String =  s"/flusso-misure/conf"
  val envVar :String =  s"/apps/deploy"

  def dir_started_driver:String=
  {
    _dir_started_driver;
  }
  def dir_started_driver_(dir:String):Unit =
  {
    _dir_started_driver=dir;
  }

  def this(start_driver_dir:String)=
  {
    this();
    this._dir_started_driver = start_driver_dir;
    this._cur_user =System.getProperty("user.name")

  }

  def PortaleConsumi34Mesi:Boolean = {

    val dir = System.getProperty("user.dir")
    if (dir == "/home/acutest/flusso-misure/bin_test")
      return true
    else
      return false
  }
  def PortaleConsumi2Mesi:Boolean = {

    val dir = System.getProperty("user.dir")
    if (dir == "/home/acutest/flusso-misure/bin_test_2m")
      return true
    else
      return false
  }


  def printEnvVar: String =
  {
    "***** Percorso lettura properties :" + envVar + " ****";
  }

  /**
   * Contiene le proprietà in formato chiave valore del file job.properties
   * @return Properties 
   */

  def prop:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    
    val is = fs.open(new Path(s"${ envVar }/job.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }
  
    /**
   * Contiene le proprietà in formato chiave valore del file query.properties
   * @return Properties 
   */
  def query:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)

    val is = fs.open(new Path(s"${ envVar }/query.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }

  /**
    * Contiene le proprietà in formato chiave valore del file query.propertiesyyyy_mm
    * per la sem
    * @return Properties
    */
  def query_sem(annomese:String):Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)

    val path:Path=if(annomese!="")
                    new Path(s"${ envVar }/sem/query/query.properties_${annomese}")
                  else
                    new Path(s"${ envVar }/sem/query/query.properties_oracle")

    val is = fs.open(path);
    val prop = new Properties();
    prop.load(is)
    prop
  }
  
  /**
   * Contiene le proprietà in formato chiave valore del file xsd.properties
   * @return Properties 
   */
    def xsdProp:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    
    val is = fs.open(new Path(s"${ envVar }/xsd.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }

  def xsdProp_old:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    val p_xsd_old = new Path(s"${envVar}/xsd.properties_vecchio")

    if (fs.exists(p_xsd_old)) {
      val is = fs.open(p_xsd_old);
      val prop = new Properties();
      prop.load(is)
      prop
    } else null
  }
  
}
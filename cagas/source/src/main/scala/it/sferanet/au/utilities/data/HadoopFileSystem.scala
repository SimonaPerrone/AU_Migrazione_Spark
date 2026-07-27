package it.sferanet.au.utilities.data


import it.sferanet.au.utilities.Environment
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkContext

import java.io._
import java.util.zip.{ZipEntry, ZipException, ZipInputStream}
import scala.collection.mutable.ListBuffer

object HadoopFileSystem {
  def pathCombine(paths: String*): String = {
    require(paths.nonEmpty)
    val build = new StringBuilder(paths.head.stripSuffix("/"))
    val other = paths.drop(1)
    for (relativePath <- other) {
      build.append("/")
      build.append(relativePath.stripSuffix("/"))
    }
    build.toString()
  }
}

class HadoopFileSystem {

  val sparkContext: SparkContext = Environment.getSparkContext
  val hdfs: FileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
  val dfs: DistributedFileSystem = hdfs match {
    case system: DistributedFileSystem => system
    case _ => null
  }

  private val master = sparkContext.getConf.get("spark.master")
  private val isLocal: Boolean = !sparkContext.getConf.contains("spark.master") || master.startsWith("local")
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)
  private lazy val recoverLeaseRetry = Environment.getProperty("hdfs.recoverLease.Retry").toInt
  private lazy val recoverLeaseDelay = Environment.getProperty("hdfs.recoverLease.Delay").toInt
  hdfs.getConf.set("dfs.support.append", true.toString)

  def openStreamFile(path: String): OutputStream = {
    val file = new Path(path)
    if (hdfs.exists(file)) {
      hdfs.delete(file, true)
    }
    val os = hdfs.create(file, true)
    os
  }

  def openFile(path: String): BufferedWriter = {
    val os = openStreamFile(path)
    hdfs.setPermission(new Path(path), FsPermission.createImmutable(777))
    val br = new BufferedWriter(new OutputStreamWriter(os))
    br
  }

  def getStream(path: String): InputStream = {
    hdfs.open(new Path(path)).getWrappedStream
  }

  /* public BufferedWriter OpenCompressorFile(String path, CompressionMode compressionMode) {
      Path file = new Path(path);
      if (hdfs.exists(file)) {
        hdfs.delete(file, true);
      }

      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(compressionMode.getCodec(), new Configuration());
      OutputStream outStream = codec.createOutputStream(hdfs.create(file, true));
      hdfs.setPermission(file, FsPermission.createImmutable((short) 0777));
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outStream));
      return br;
    } */

  //  private def appendFileHDFS(path: String): BufferedWriter = {
  //    this.appendFileHDFS(path, createIfNotExist = false)
  //  }

  private def appendFileHDFS(path: String, createIfNotExist: Boolean): BufferedWriter = {
    val hdfsPath = new Path(path)
    if (hdfs.exists(hdfsPath)) {
      if (isLocal) {
        val fr = new FileWriter(path, true)
        new BufferedWriter(fr)
      } else {
        val outStream = hdfs.append(hdfsPath)
        new BufferedWriter(new OutputStreamWriter(outStream))
      }
    } else {
      if (createIfNotExist) {
        val file = hdfs.create(hdfsPath)
        new BufferedWriter(new OutputStreamWriter(file))
      } else {
        throw new FileNotFoundException("File %s in hdfs not found".format(path))
      }
    }
  }

  def appendLine(path: String, content: String): Unit = {
    this.synchronized {
      if (!this.recoverLease(path))
        log.warn("file not recovered %s. End retry".format(path))
      val file = this.appendFileHDFS(path, createIfNotExist = true)
      try {
        file.write(content + "\n")
      } finally {
        file.flush()
        file.close()
      }
    }
  }

  private def recoverLease(path: String): Boolean = {
    val hdfsPath = new Path(path)
    for (_ <- 1 to this.recoverLeaseRetry) {
      try {
        val recovered = dfs.recoverLease(hdfsPath)
        if (dfs == null || recovered)
          true
        else {
          log.warn("file not recovered %s. Retry".format(path))
          Thread.sleep(this.recoverLeaseDelay)
        }
      } catch {
        case ex: Throwable =>
          log.info("Exception to recoverLease %s: %s. Retry".format(path, ex.getMessage))
      }
    }
    false
  }

  def move(src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    if (!hdfs.exists(dstPath))
      hdfs.mkdirs(dstPath)

    val status = hdfs.globStatus(srcPath)
    val paths = FileUtil.stat2Paths(status)
    for (p <- paths) {
      hdfs.rename(p, dstPath)
    }
  }

  def copy(src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    if (!hdfs.exists(dstPath))
      hdfs.mkdirs(dstPath)

    val status = hdfs.globStatus(srcPath)
    val paths = FileUtil.stat2Paths(status)
    for (p <- paths) {
      hdfs.copyFromLocalFile(p, dstPath)
    }
  }

  def copyFile(src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    if (!hdfs.exists(dstPath.getParent))
      hdfs.mkdirs(dstPath.getParent)

    hdfs.copyFromLocalFile(srcPath, dstPath)
  }

  def deleteIfExist(path: String): Unit = {
    val p = new Path(path)
    log.info("delete file %s".format(path))
    if (hdfs.exists(p))
      hdfs.delete(p, true)
  }

  def deleteIfExist(paths: Iterable[String]): Unit = {
    for (i <- paths)
      deleteIfExist(i)
  }

  def listFiles(path: String): Array[String] = {
    val retVal = hdfs.globStatus(new Path(path))
    if (retVal == null)
      new Array[String](0)
    else
      retVal.map {
        x => x.getPath.toString
      }
  }

  def close(): Unit = {
    // hdfs.close()
  }

  def unZipIt(zipFile: String, outputFolder: String): Seq[String] = {
    val zis: ZipInputStream = new ZipInputStream(this.getStream(zipFile))
    try {
      val retVal = new ListBuffer[String]

      var ze: ZipEntry = zis.getNextEntry
      while (ze != null) {
        val fileName = (if (ze.getName == "-") "out" else ze.getName).replaceAll(" ", "_")
        val f = new File(outputFolder, fileName)
        val newFile = f.toString
        log.info("file unzip : %s".format(f.getAbsoluteFile))

        new File(outputFolder).mkdirs()
        val fos = this.openStreamFile(newFile)
        try {
          if (ze.getMethod == ZipEntry.DEFLATED) {
            //            val deflater = new InflaterInputStream(zis)
            //            IOUtils.copyBytes(deflater, fos, 1024 * 1024, false)
            try {
              IOUtils.copyBytes(zis, fos, 1024 * 1024, false)
            } catch {
              case e: ZipException =>
                log.warn("exception caught: " + e.getMessage)
            }
            ze = zis.getNextEntry
            // deflater.close()
          } else {
            IOUtils.copyBytes(zis, fos, 1024 * 1024, false)
            ze = zis.getNextEntry
          }
        } finally {
          fos.close()
        }
        retVal.append(new Path(newFile).toString)
      }
      retVal
    } catch {
      case e: IOException =>
        log.error("exception caught: " + e.getMessage)
        throw e
    } finally {
      // zis.closeEntry()
      zis.close()
    }
  }
}
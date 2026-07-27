package it.sferanet.au.utilities

import org.apache.hadoop.fs.Path

object HDFSUtils {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def deleteIfExist(path: String): Unit = {
    val p = new Path(path)
    log.info("delete file %s".format(path))
    if (Environment.getFs.exists(p))
      Environment.getFs.delete(p, true)
  }
}

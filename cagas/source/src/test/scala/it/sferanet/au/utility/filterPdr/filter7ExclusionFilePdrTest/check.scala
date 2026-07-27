package it.sferanet.au.utility.filterPdr.filter7ExclusionFilePdrTest

import it.sferanet.au.model.Flow
import it.sferanet.au.schema.PdrMassivoSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.junit.Assert

/**
 * fixme: mettere a fattor comune nel caso in cui altri test possano usare queste funzioni
 */
object check {
  /**
   *
   * @param df       dataframe su cui effettuare il controllo
   * @param excepted true se il pdr dovrà essere presente dopo il filtro
   *                 false altrimenti
   * @param codPdr   cod_pdr da controllare
   */
  def checkIfExsist(df: DataFrame, excepted: Boolean, codPdr: String): Unit = {
    Assert.assertEquals(if (excepted) 1 else 0, df.where(col("codice_pdr") === codPdr).dropDuplicates(Seq("codice_pdr")).count())
  }

  /**
   *
   * @param rdd      Rdd di Flow su cui effettuare il controllo
   * @param excepted true se il pdf dovrà essere presente dopo il filtro
   *                 false altrimenti
   * @param codPdr   cod_pdr da controllare
   */
  def checkIfExsist(rdd: RDD[Flow], excepted: Boolean, codPdr: String): Unit = {
    Assert.assertEquals(if (excepted) 1 else 0, rdd.filter(_.pdr == codPdr).distinct().count())
  }

  /**
   *
   * @param flow               Rdd di @link Flow su cui effettuare il controllo
   * @param codPdr             pdr da controllare
   * @param numberFlowAspected numero di flow aspettato
   */
  def checkNumberFlow(flow: RDD[Flow], codPdr: String, numberFlowAspected: Int): Unit = {
    Assert.assertEquals(numberFlowAspected, flow.filter(_.pdr == codPdr).count())
  }

  /**
   *
   * @param df                 dataframe su cui effettuare il controllo
   * @param codPdr             pdr da controllare
   * @param numberFlowAspected numero di flow aspettato
   */
  def checkNumberFlow(df: DataFrame, codPdr: String, numberFlowAspected: Int): Unit = {
    Assert.assertEquals(numberFlowAspected, df.filter(col("codice_pdr") === codPdr).count())
  }

  /**
   *
   * @param df      dataframe su cui effettuare il controllo
   * @param codPdr  da controllare
   * @param ca      ca aspettato per @link codPdr
   * @param codPrel codPrel aspettato per @link codPdr
   */
  def checkConstantField(df: DataFrame, codPdr: Int, ca: Double, codPrel: String): Unit = {
    Assert.assertEquals(ca, df.filter(col(PdrMassivoSchema.codice_pdr) === lit(codPdr)).collect()(0)(1))
    Assert.assertEquals(codPrel, df.filter(col(PdrMassivoSchema.codice_pdr) === lit(codPdr)).collect()(0)(2))
  }

}

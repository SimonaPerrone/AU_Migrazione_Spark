package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.FlowCdpDatiPrelievoGas
import it.eng.au.aggregatoreConsumiCdp.dao.cdp.{CaDao, CaPreFinalDao}
import it.eng.au.aggregatoreConsumiCdp.schema.{CaPreFinalSchema, CaSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

object CaDedotti extends FlowCdpDatiPrelievoGas {
  override def addPiva(caFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame): DataFrame = {
    caFinal
  }

  override def specificTransform(caFinal: DataFrame): DataFrame = {
    val caExecutionId = getCaExecutionId
    val ca = new CaDao().readPartition(caExecutionId)
    val caPreFinal = new CaPreFinalDao().readPartition(caExecutionId)

    val caPrepared = prepareCa(ca)
    val preparedCaPreFinal = prepareCaPreFinal(caPreFinal)

    //join ca_pre_final with ca with field pdr. pdr should be unique left join
    val df = preparedCaPreFinal
      .join(caPrepared, col(CaPreFinalSchema.codice_pdr) === col(CaSchema.pdr), "left")
      .drop(CaSchema.pdr)
      .withColumn(OutputHiveSchema.causale, coalesce(col(CaPreFinalSchema.calcmode), col(CaSchema.idcaerrorcode)))
      .drop(CaPreFinalSchema.calcmode)
      .drop(CaSchema.idcaerrorcode)
      .withColumn(OutputHiveSchema.causale, when(col(OutputHiveSchema.causale).isNull, lit("M1")).otherwise(col(OutputHiveSchema.causale)))

    //join ca_final with df before with pdr is a inner join
    val res = caFinal
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(concat(lit("01/10/"), col(OutputHiveSchema.anno_competenza) - 1), DATA_DECORRENZA_FORMAT), TIMESTAMP_FORMAT))
      .withColumn(OutputHiveSchema.udd_oggetto_swithcing, lit(""))
      .drop(OutputHiveSchema.causale) //it needs to be dropped because there is already a causale in the second df..could cause ambiguity
      .join(df, col(OutputHiveSchema.cod_pdr) === col(CaPreFinalSchema.codice_pdr), "inner")
      .drop(CaPreFinalSchema.codice_pdr)

    res
  }

  //filter caPreFinal only the record that you need, filter calc_mode like "con forzatura per valori anomali"
  //after gropup by with pdr with only pdr and calc_mode or if you want add now causale with DF

  /**
   * this method serves to prepare the CaPreFinal dataframe for the following computations.
   * 1.step: filters only rows with calcmode like "con forzatura per valori anomali"
   * 2.step: the remaining rows set calcmode = "DF"
   *
   * the input and the output should look like this:
   *
   * The Input
   * +----------+---------------+------------------------------------------+
   * |codice_pdr|other_fileds...|calcmode                                  |
   * +----------+---------------+------------------------------------------+
   * |codicepdr1|...            |dedotto                                   |
   * |codicepdr1|...            |procedura                                 |
   * |codicepdr2|...            |procedura                                 |
   * |codicepdr2|...            |dedotto con forzatura per valori anomali  |
   * |codicepdr3|...            |procedura                                 |
   * +----------+---------------+------------------------------------------+
   *
   * The Output
   * +-----------+----------+
   * | codice_pdr|calcmode  |
   * +-----------+----------+
   * | codicepdr1| null     |
   * | codicepdr2| DF       |
   * +-----------+----------+
   *
   * */
  def prepareCaPreFinal(caPreFinal: DataFrame): DataFrame = {
    caPreFinal
      .where(col(CaPreFinalSchema.prelievo_annuo_prev_forced).isNull)
      .select(CaPreFinalSchema.codice_pdr, CaPreFinalSchema.calcmode)
      .filter(col(CaPreFinalSchema.calcmode).like("%dedotto%"))
      .withColumn(CaPreFinalSchema.calcmode, when(col(CaPreFinalSchema.calcmode).like("%con forzatura per valori anomali%"), lit("DF")))
      .distinct()
  }

  /**
   * this method serves to prepare the Ca dataframe for the following computations. this method works in three steps
   * 1.step:  if for a given pdr, there is at least one record with idCaErrorCode = 0, remove this pdr
   * 2.step: converts idcaerrorcode to the appropriate value, eg: 1 -> M1
   * 3.step: filters duplicates pdr with this order of priority idcaerrorcode: DF -> M1 -> M2 -> M3 -> M4 -> T1 -> T2
   * */
  def prepareCa(ca: DataFrame): DataFrame = {
    val caRes = ca
      .select(CaSchema.pdr, CaSchema.idcaerrorcode)
      .withColumn("min", min(CaSchema.idcaerrorcode).over(Window.partitionBy(CaSchema.pdr)))
      .filter(col("min") =!= 0).drop("min")
      .withColumn("priority", udfPriority(col(CaSchema.idcaerrorcode)))
      .groupBy(col(CaSchema.pdr)).agg(min(col("priority")).as("priority"))
      .withColumn(CaSchema.idcaerrorcode, udfMappingCode(col("priority")))
      .drop("priority")
    caRes
  }

  val udfPriority: UserDefinedFunction = udf((errorCode: String) => errorCode match {
    case "1" => "1"
    case "2" => "6"
    case "3" => "1"
    case "4" => "5"
    case "5" => "1"
    case "6" => "6"
    case "7" => "3"
    case "8" => "4"
    case "9" => "2"
    case _ => "1"
  })

  val udfMappingCode: UserDefinedFunction = udf((errorCode: String) => errorCode match {
    case "1" => "M1"
    case "2" => "M2"
    case "3" => "M3"
    case "4" => "M4"
    case "5" => "T1"
    case "6" => "T2"
    case _ => "M1"
  })
}

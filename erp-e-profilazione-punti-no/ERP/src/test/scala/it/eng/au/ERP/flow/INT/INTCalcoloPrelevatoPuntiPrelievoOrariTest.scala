package it.eng.au.ERP.flow.INT

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.schema.erp.erpValidatedIntSchema
import it.eng.au.ERP.schema.rcu.rcuPodPSchema
import it.eng.au.ERP.trasformations.INT.CalcoloPrelevatoPuntiPrelievoOrariINT
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.environment.Environment
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Assert

class INTCalcoloPrelevatoPuntiPrelievoOrariTest  extends EnvironmentSparkTest{
 implicit  val spark = Environment.getSpark

//  @Ignore("not ready yet")
  def testDfFlussoMisureInterconnessioneEnrichedFiltered(): Unit = {
    // Create an instance with all doubles = 1.0
    import spark.implicits._

    val area = rcuPodPSchema.t_area_rif

    val schema = StructType(Seq(
      StructField(area, StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    // Create data
    val data = Seq(
      Row("area1", "Alice"),
      Row("area2", "Bob")
    )

    // Create DataFrame
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)

    val dfCountArea1 = df.filter(col(area) === "area1").count()

//    val arg1 = ERPArgsConfig(area=Some("area1"))
    val arg1=Some("area1")


//    val arg2 = ERPArgsConfig(area=None)
    val arg2=None

    val finalDf1 = CalcoloPrelevatoPuntiPrelievoOrariINT
      .dfFlussoMisureInterconnessioneFilteredArea(df,arg1)

    Assert.assertEquals(dfCountArea1, finalDf1.count())

    finalDf1.show()

    val finalDf2 = CalcoloPrelevatoPuntiPrelievoOrariINT
      .dfFlussoMisureInterconnessioneFilteredArea(df,arg2)

    Assert.assertEquals(df.count(), finalDf2.count())
    finalDf2.show()

  }

  def testDfErpValidatoIntFilteredPiva(): Unit = {
    // Create an instance with all doubles = 1.0
    import spark.implicits._

    val piva1 = erpValidatedIntSchema.pivagdrmis
    val piva2 = erpValidatedIntSchema.pivagdrinst
    val piva3 = erpValidatedIntSchema.pivagdralt

    val pivaDataOk = "123"
    val pivaDataNotOk = "789"


    val schema = StructType(Seq(
      StructField(piva1, StringType, nullable = true),
      StructField(piva2, StringType, nullable = true),
      StructField(piva3, StringType, nullable = true),
      StructField("name", StringType, nullable = false)
    ))

    // Create data
    val data = Seq(
      Row("123","345",null, "Alice"),
      Row("123","123","123", "Alice"),
      Row("123a","345",null, "Alice"),
      Row("123b","345","dfg", "Alice"),
      Row("123b","345","123", "Alice"),
      Row("123b","123","123", "Alice"),
      Row(null,null,null, "Alice"),
      Row(null,"345",null, "Alice")
    )

    // Create DataFrame
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)

    val dfCountOk = df
      .filter(col(piva1)===pivaDataOk ||
        col(piva2)===pivaDataOk ||
        col(piva3)===pivaDataOk
      )
      .count()

    val dfCountNotOk = df
      .filter(col(piva1)===pivaDataNotOk ||
        col(piva2)===pivaDataNotOk ||
        col(piva3)===pivaDataNotOk
      )
      .count()

//    val arg1Ok = ERPArgsConfig(singola_piva_distributore=Some(pivaDataOk))
    val arg1Ok =Some(pivaDataOk)


//    val arg1NotOk = ERPArgsConfig(singola_piva_distributore=Some(pivaDataNotOk))
    val arg1NotOk =Some(pivaDataNotOk)


//    val arg2 = ERPArgsConfig(singola_piva_distributore=None)
    val arg2 =None

    val finalDf1Ok = CalcoloPrelevatoPuntiPrelievoOrariINT
      .dfErpValidatoIntFilteredPiva(df,arg1Ok)

    Assert.assertEquals(dfCountOk, finalDf1Ok.count())

    finalDf1Ok.show()

///////////////////////
    val finalDf1NotOk = CalcoloPrelevatoPuntiPrelievoOrariINT
        .dfErpValidatoIntFilteredPiva(df,arg1NotOk)

    Assert.assertEquals(dfCountNotOk, finalDf1NotOk.count())

    finalDf1NotOk.show()


    val finalDf2 = CalcoloPrelevatoPuntiPrelievoOrariINT
      .dfErpValidatoIntFilteredPiva(df,arg2)

    Assert.assertEquals(df.count(), finalDf2.count())
    finalDf2.show()

  }

  def testFunzionePivaCasi(): Unit = {
    import spark.implicits._

    val piva1 = erpValidatedIntSchema.pivagdrmis
    val piva2 = erpValidatedIntSchema.pivagdrinst
    val piva3 = erpValidatedIntSchema.pivagdralt

    val pivaData1 = "123"
    val pivaData2 = "456"
    val pivaData3 = "789"
    val casoPIVA = "casoPIVA"


    val schema = StructType(Seq(
      StructField(piva1, StringType, nullable = true),
      StructField(piva2, StringType, nullable = true),
      StructField(piva3, StringType, nullable = true),
      StructField("name", StringType, nullable = false)
    ))

    // Create data
    val data = Seq(
      //case1
      Row(pivaData2,pivaData3,pivaData1, "Alice1"),
      Row(pivaData3,pivaData2,pivaData1, "Alice1"),
      Row(pivaData1,pivaData3,pivaData2, "Alice1"),
      Row(pivaData3,pivaData1,pivaData2, "Alice1"),
      Row(pivaData1,pivaData2,pivaData3, "Alice1"),
      Row(pivaData2,pivaData1,pivaData3, "Alice1"),
      //case2
      Row(pivaData2,pivaData2,pivaData1, "Alice2"),
      Row(pivaData3,pivaData3,pivaData1, "Alice2"),
      Row(pivaData1,pivaData1,pivaData2, "Alice2"),
      Row(pivaData3,pivaData3,pivaData2, "Alice2"),
      Row(pivaData1,pivaData1,pivaData3, "Alice2"),
      Row(pivaData2,pivaData2,pivaData3, "Alice2"),
      //case3
      Row(pivaData1,pivaData2,pivaData1, "Alice3"),
      Row(pivaData1,pivaData3,pivaData1, "Alice3"),
      Row(pivaData2,pivaData1,pivaData2, "Alice3"),
      Row(pivaData2,pivaData3,pivaData2, "Alice3"),
      Row(pivaData3,pivaData1,pivaData3, "Alice3"),
      Row(pivaData3,pivaData2,pivaData3, "Alice3"),
      //case4
      Row(pivaData2,pivaData1,pivaData1, "Alice4"),
      Row(pivaData3,pivaData1,pivaData1, "Alice4"),
      Row(pivaData1,pivaData2,pivaData2, "Alice4"),
      Row(pivaData3,pivaData2,pivaData2, "Alice4"),
      Row(pivaData1,pivaData3,pivaData3, "Alice4"),
      Row(pivaData2,pivaData3,pivaData3, "Alice4"),
      //case5
      Row(pivaData1,pivaData1,pivaData1, "Alice5"),
      Row(pivaData2,pivaData2,pivaData2, "Alice5"),
      Row(pivaData3,pivaData3,pivaData3, "Alice5")

    )

    // Create DataFrame
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)

    val finalDf = df.withColumn(casoPIVA
      ,CalcoloPrelevatoPuntiPrelievoOrariINT
        .funzionePivaCasi(
          col(erpValidatedIntSchema.pivagdrmis),
          col(erpValidatedIntSchema.pivagdrinst),
          col(erpValidatedIntSchema.pivagdralt))
    )

    val finalDf1 = finalDf.filter(col(casoPIVA)===1).count()
    val finalDf2 = finalDf.filter(col(casoPIVA)===2).count()
    val finalDf3 = finalDf.filter(col(casoPIVA)===3).count()
    val finalDf4 = finalDf.filter(col(casoPIVA)===4).count()
    val finalDf5 = finalDf.filter(col(casoPIVA)===5).count()

    val df1 = df.filter(col("name") ==="Alice1").count()
    val df2 = df.filter(col("name") ==="Alice2").count()
    val df3 = df.filter(col("name") ==="Alice3").count()
    val df4 = df.filter(col("name") ==="Alice4").count()
    val df5 = df.filter(col("name") ==="Alice5").count()

    Assert.assertEquals(df1, finalDf1)
    Assert.assertEquals(df2, finalDf2)
    Assert.assertEquals(df3, finalDf3)
    Assert.assertEquals(df4, finalDf4)
    Assert.assertEquals(df5, finalDf5)




  }

  }

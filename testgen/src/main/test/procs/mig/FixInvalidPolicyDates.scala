package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import scala.collection.Parallelizable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import com.hig.epp.utils.EppUtils

case class FixInvalid(riskStateCd: Long, ratingPlan: String, regionalOfficeOrgId: Int, plcySymbolCd: String, plcyNbr: String,
  plcyPeriodId: Long, plcyEfdt: Int, originalPlcyEfdt: Int, plcyExdt: Int)

object FixInvalidPolicyDates {

  val LOG = LoggerFactory.getLogger("FixInvalidPolicyDates")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new FixInvalid(42, "PLIC", 42, "PH", "723527", 240132159, 19431200, 19561206, 19561506),
      new FixInvalid(42, "PLIC", 55, "PH", "723527", 240232159, 19431203, 19431200, 19561206),
      new FixInvalid(42, "PLIC", 42, "PH", "723527", 240332159, 19430012, 0, 19561206),
      new FixInvalid(42, "PLIC", 55, "PH", "723527", 240432159, 19430003, 19590831, 19560006),
      new FixInvalid(84, "PLIC", 64, "PH", "723527", 240532159, 19431135, 19430003, 19561206),
      new FixInvalid(84, "PLIC", 64, "PH", "723527", 240632159, 19430044, 19591400, 19561200),
      new FixInvalid(42, "PLIC", 42, "PH", "723527", 240732159, 19430000, 19590003, 19561206),
      new FixInvalid(84, "PLIC", 64, "PH", "723527", 240832159, 19430831, 0, 19561206),
      new FixInvalid(42, "PLIC", 42, "PH", "723527", 240932159, 19431132, 19591223, 19560000),
      new FixInvalid(84, "PLIC", 64, "PH", "723527", 241032159, 19430803, 19591108, 19561206))

    //System.setProperty("hadoop.home.dir", "C:/Users/co24670/Documents/Projects/SEPP/work")
    
    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    //val sqlContext: SQLContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    val dtChk = udf(new EppUtils().isValidDate _)

    val isValidPlcyDate = dtChk(col("plcyEfdt"), lit("yyyyMMdd"))
    val inValidPlcyDay = col("plcyEfdt").substr(7, 2) === "00"
    val inValidPlcyMonth = col("plcyEfdt").substr(5, 2) === "00"
    val plcyEftdtTillMonth = col("plcyEfdt").substr(0, 6)
    val plcyEfdtYear = col("plcyEfdt").substr(0, 4)
    val plcyEftdtDay = col("plcyEfdt").substr(7, 2)
    val validPlcyDtDay = (col("plcyEfdt") + 15).cast("Int")
    val validPlcyDtMonth = ((plcyEfdtYear * 10000) + 600 + plcyEftdtDay).cast("Int")
    val validPlcyMonthDay = ((plcyEfdtYear * 10000) + 600 + 15).cast("Int")

    val isValidOrigPlcyDate = dtChk(col("originalPlcyEfdt"), lit("yyyyMMdd"))
    val inValidOrigPlcyDay = col("originalPlcyEfdt").substr(7, 2) === "00"
    val inValidOrigPlcyMonth = col("originalPlcyEfdt").substr(5, 2) === "00"
    val origPlcyEfdtTillMonth = col("originalPlcyEfdt").substr(0, 6)
    val origPlcyEfdtYear = col("originalPlcyEfdt").substr(0, 4)
    val origPlcyEftdtDay = col("originalPlcyEfdt").substr(7, 2)
    val validOrigPlcyDtDay = (col("originalPlcyEfdt") + 15).cast("Int")
    val validOrigPlcyDtMonth = ((origPlcyEfdtYear * 10000) + 600 + origPlcyEftdtDay).cast("Int")
    val validOrigPlcyMonthDay = ((origPlcyEfdtYear * 10000) + 600 + 15).cast("Int")

    val isValidPlcyExDate = dtChk(col("plcyExdt"), lit("yyyyMMdd"))
    val inValidPlcyExDay = col("plcyExdt").substr(7, 2) === "00"
    val inValidPlcyExMonth = col("plcyExdt").substr(5, 2) === "00"
    val plcyExtdtTillMonth = col("plcyExdt").substr(0, 6)
    val plcyExdtYear = col("plcyExdt").substr(0, 4)
    val plcyExtdtDay = col("plcyExdt").substr(7, 2)
    val validPlcyExDtDay = (col("plcyExdt") + 15).cast("Int")
    val validPlcyExDtMonth = ((plcyExdtYear * 10000) + 600 + plcyExtdtDay).cast("Int")
    val validPlcyExMonthDay = ((plcyExdtYear * 10000) + 600 + 15).cast("Int")

    val oDF = inDF.withColumn("updPlcyDt", when(!(isValidPlcyDate) and inValidPlcyDay and inValidPlcyMonth, validPlcyMonthDay)
      .when(!(isValidPlcyDate) and inValidPlcyDay, validPlcyDtDay)
      .when(!(isValidPlcyDate) and inValidPlcyMonth, validPlcyDtMonth)
      .otherwise(col("plcyEfdt")))
      .withColumn("updOrigPlcyEfDt", when(!(isValidOrigPlcyDate) and inValidOrigPlcyDay and inValidOrigPlcyMonth, validOrigPlcyMonthDay)
        .when(!(isValidOrigPlcyDate) and inValidOrigPlcyDay, validOrigPlcyDtDay)
        .when(!(isValidOrigPlcyDate) and inValidOrigPlcyMonth, validOrigPlcyDtMonth)
        .otherwise(col("originalPlcyEfdt")))
      .withColumn("updPlcyExDt", when(!(isValidPlcyExDate) and inValidPlcyExDay and inValidPlcyExMonth, validPlcyExMonthDay)
        .when(!(isValidPlcyExDate) and inValidPlcyExDay, validPlcyExDtDay)
        .when(!(isValidPlcyExDate) and inValidPlcyExMonth, validPlcyExDtMonth)
        .otherwise(col("plcyExdt")))

    oDF.show()

    val isNotNullUpdOrigPlcyDate = col("updOrigPlcyEfDt") > 0 
    val o1DF = oDF.filter(isNotNullUpdOrigPlcyDate)
                  .groupBy(col("riskStateCd"), col("ratingPlan"), col("regionalOfficeOrgId"), col("plcySymbolCd"), col("plcyNbr"))
                  .agg(max($"plcyPeriodId") as "maxPlcyPeriodId")
                  //.join(oDF.select($"originalPlcyEfdt") as "MaxPeriodOrigPlcyEfDt", oDF("plcyPeriodId") === $"maxPlcyPeriodId", "left_outer")
   
    o1DF.show()
    val o2DF = o1DF.join(oDF.select(col("riskStateCd") as "oDFriskStateCd"
                                    ,col("ratingPlan") as "oDFratingPlan"
                                    ,col("regionalOfficeOrgId") as "oDFregionalOfficeOrgId"
                                    ,col("plcySymbolCd") as "oDFplcySymbolCd"
                                    ,col("plcyNbr") as "oDFplcyNbr"
                                    ,col("plcyPeriodId") as "oDFplcyPeriodId"
                                    ,col("originalPlcyEfdt") as "oDForiginalPlcyEfdt"), 
                              col("oDFriskStateCd") === o1DF.col("riskStateCd") 
                            && col("oDFratingPlan") === o1DF.col("ratingPlan")
                            && col("oDFregionalOfficeOrgId") === o1DF.col("regionalOfficeOrgId")
                            && col("oDFplcySymbolCd") === o1DF.col("plcySymbolCd")
                            && col("oDFplcyNbr") === o1DF.col("plcyNbr")
                            && col("oDFplcyPeriodId") === o1DF.col("maxPlcyPeriodId") )
                            .drop("oDFriskStateCd")
                            .drop("oDFratingPlan")
                            .drop("oDFregionalOfficeOrgId")
                            .drop("oDFplcySymbolCd")
                            .drop("oDFplcyNbr")
                            .drop("oDFplcyPeriodId")

    o2DF.show()
    val isValidupdOrigPlcyEfDt = col("updOrigPlcyEfDt") === 0
    val o4DF = oDF.join(o2DF.select(col("riskStateCd") as "o2DFriskStateCd"
                                    ,col("ratingPlan") as "o2DFratingPlan"
                                    ,col("regionalOfficeOrgId") as "o2DFregionalOfficeOrgId"
                                    ,col("plcySymbolCd") as "o2DFplcySymbolCd"
                                    ,col("plcyNbr") as "o2DFplcyNbr"
                                    ,col("maxPlcyPeriodId") as "o2DFmaxPlcyPeriodId"
                                    ,col("oDForiginalPlcyEfdt") as "o2DFMaxoriginalPlcyEfdt"), 
                              col("o2DFriskStateCd") === oDF.col("riskStateCd") 
                            && col("o2DFratingPlan") === oDF.col("ratingPlan")
                            && col("o2DFregionalOfficeOrgId") === oDF.col("regionalOfficeOrgId")
                            && col("o2DFplcySymbolCd") === oDF.col("plcySymbolCd")
                            && col("o2DFplcyNbr") === oDF.col("plcyNbr")
                , "left_outer")
                .drop("o2DFriskStateCd")
                .drop("o2DFratingPlan")
                .drop("o2DFregionalOfficeOrgId")
                .drop("o2DFplcySymbolCd")
                .drop("o2DFplcyNbr")
                .drop("o2DFplcyPeriodId")
                .withColumn("Upd2OriginalPlcyEftDt", when(isValidupdOrigPlcyEfDt,col("o2DFMaxoriginalPlcyEfdt")).otherwise(col("updOrigPlcyEfDt")))
                
    o4DF.show()
    LOG.info("Ended")

  }
}

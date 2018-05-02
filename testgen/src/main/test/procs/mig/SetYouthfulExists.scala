package com.hig.epp.sunproc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.slf4j.LoggerFactory

object SetYouthfulExists {

  case class MpdAutoDriverYE(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcyVehicleId: Long, plcyDriverId: Long, driverAge: Int)
  case class MpdAutoVehicleYE(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, plcyVehicleId: Long, youthfulExists: String, LvehRatedTypeCd: String)
  case class MpdAutoPolicyPeriod(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Long, youthfulExistsPP: String)

  def main(args: Array[String]) {
    val LOG = LoggerFactory.getLogger("SetYouthfulExists")
    val inData = Seq(
      new MpdAutoDriverYE(42, "PLIC", 240132159, 19431200, 240132001, 10),
      new MpdAutoDriverYE(42, "PLIC", 240132200, 19431200, 240132002, 18),
      new MpdAutoDriverYE(42, "PLIC", 240132159, 19431200, 240132003, 15),
      new MpdAutoDriverYE(42, "PLIC", 240132200, 19431200, 240132004, 19),
      new MpdAutoDriverYE(42, "PLIC", 240132159, 19431200, 240132005, 20),
      new MpdAutoDriverYE(42, "PLIC", 240132159, 19431200, 0, 10),
      new MpdAutoDriverYE(42, "PLID", 240132159, 19431200, 240132006, 26))

    val inDataGrp = Seq(
      new MpdAutoVehicleYE(42, "PLIC", 240132159, 19431200, "N", "1"),
      new MpdAutoVehicleYE(42, "PLIC", 240132200, 19431200, "N", "2"),
      new MpdAutoVehicleYE(42, "PLIC", 240132159, 19431200, "N", "3"),
      new MpdAutoVehicleYE(42, "PLIC", 240132200, 19431200, "N", "1"),
      new MpdAutoVehicleYE(42, "PLIC", 240132159, 19431200, "N", "4"),
      new MpdAutoVehicleYE(42, "PLIC", 240132159, 19431200, "N", "1"),
      new MpdAutoVehicleYE(52, "PLID", 240132159, 19431200, "N", "2"))

        val inAutoPlcyPeriod = Seq(
      new MpdAutoPolicyPeriod(42, "PLIC", 240132159, "N"),
      new MpdAutoPolicyPeriod(42, "PLIC", 240132159, "N"),
      new MpdAutoPolicyPeriod(52, "PLIC", 240132159, "N"))

      
    val sparkConf = new SparkConf().setAppName("Youtful").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    import sqlContext.implicits._

    val inDataDFGrp = inDataGrp.toDF()
    val inDataDF = inData.toDF()
    
    val inAutoPlcyPeriodDF = inAutoPlcyPeriod.toDF()
    

    val validPlcyDriverId = col("plcyDriverId") > 0
    val youthDriverAge = col("driverAge") < 25
    val outDataDF = inDataDF.filter(validPlcyDriverId and youthDriverAge)
      .groupBy(col("riskStateCd"), col("ratingPlan"), col("plcyPeriodId"), col("plcyVehicleId")).count()

    outDataDF.show()

    val validRatedTypeCd = col("LvehRatedTypeCd") === "1" or col("LvehRatedTypeCd") === "2"
    val validOutriskStateCd = col("OutriskStateCd") >= 0

    val out1DF = inDataDFGrp.join(outDataDF.select(col("riskStateCd") as "OutriskStateCd", col("ratingPlan") as "OutratingPlan", col("plcyPeriodId") as "OutplcyPeriodId", col("plcyVehicleId") as "OutplcyVehicleId"),
      col("riskStateCd") === col("OutriskStateCd")
        && col("ratingPlan") === col("OutratingPlan")
        && col("plcyPeriodId") === col("OutplcyPeriodId")
        && col("plcyVehicleId") === col("OutplcyVehicleId"), "left_outer")
      .withColumn("UpdYouthFulExists", when(validOutriskStateCd and validRatedTypeCd, lit("Y")).otherwise(col("youthfulExists")))

    out1DF.show()

    val yesYouthFulExists = col("UpdYouthFulExists") === "Y"

    val out2DF = out1DF.filter(yesYouthFulExists).groupBy(col("riskStateCd"), col("ratingPlan"), col("plcyPeriodId")).count()

    val out4DF = inAutoPlcyPeriodDF.join(out2DF.select(col("riskStateCd") as "OutriskStateCd", col("ratingPlan") as "OutratingPlan", col("plcyPeriodId") as "OutplcyPeriodId"),
      col("riskStateCd") === col("OutriskStateCd")
        && col("ratingPlan") === col("OutratingPlan")
        && col("plcyPeriodId") === col("OutplcyPeriodId") , "left_outer")
      .withColumn("UpdYouthFulExistsPP", when(validOutriskStateCd , lit("Y")).otherwise(col("youthfulExistsPP")))
    
      out4DF.show()
  }
}

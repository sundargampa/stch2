package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import scala.collection.Parallelizable
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

import java.sql.Date
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils

case class mpdAutoVehicleNAMM(riskStateCd: Long, ratingPlan: String, ratedPlcyDriverId: BigDecimal, lvehRatedTypeCd: String, mctdInd :String)

object NonAgcyMctdMap {

  val LOG = LoggerFactory.getLogger("NonAgcyMctdMap")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "1","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "2","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", null, "3","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "1","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", null, "2",null),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203,"4" ,"A"),
      new mpdAutoVehicleNAMM(42L, "PLIC", null, "1","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "2","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "1","N"),
      new mpdAutoVehicleNAMM(42L, "PLIC", 19431203, "1","N"))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    val isNullDriverId = col("ratedPlcyDriverId").isNull
    val inlvehRatedTypeCd = col("lvehRatedTypeCd") === "1" or col("lvehRatedTypeCd") === "2"
    val allConditionMet = isNullDriverId && inlvehRatedTypeCd

    val OutDF = inDF.withColumn("UpdDriverId", when(allConditionMet, 999999999999L).otherwise(col("ratedPlcyDriverId").cast(LongType)))
                    .withColumn("UpdMctdInd", when(allConditionMet, lit("Y")).otherwise(col("mctdInd")))

    LOG.info("Out DF Schema: {}", OutDF.schema)

    OutDF.show()
    LOG.info("Ended")

  }
}

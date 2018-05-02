package com.hig.epp.sunproc

import org.slf4j.LoggerFactory
import scala.collection.Parallelizable
import scala.collection.JavaConverters._
import scala.collection.immutable.StringOps

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.functions.lpad
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkConf

case class UpdateNonRecalcVegAge(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Int, lvehModelYr: Int, non_recalculated_veh_age: String)
case class AutoPolicyPeriod(riskStateCd: Long, ratingPlan: String, plcyPeriodId: Int, originalPolicyEffDt: Int)

object UpdateNonRecalcVegAge {

  val LOG = LoggerFactory.getLogger("UpdateNonRecalcVegAge")

  def main(args: Array[String]) {
    LOG.info("Started")

    val inData = Seq(
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007131, 1797, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007131, 1997, "16"),
      new UpdateNonRecalcVegAge(42L, "PLIC", 222007131, 1917, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007131, 1918, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007131, 2017, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 444407131, 1997, "16"),
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007131, 2015, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 222007131, 2000, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 190007144, 2007, ""),
      new UpdateNonRecalcVegAge(42L, "PLIC", 444407131, 1997, "16"))

    val policyPeriod = Seq(
      new AutoPolicyPeriod(42L, "PLIC", 190007131, 20170630),
      new AutoPolicyPeriod(42L, "PLIC", 190007232, 20160502),
      new AutoPolicyPeriod(42L, "PLIC", 222007631, 20180230),
      new AutoPolicyPeriod(42L, "PLIC", 444407831, 20150430),
      new AutoPolicyPeriod(42L, "PLIC", 222007131, 20130830),
      new AutoPolicyPeriod(42L, "PLIC", 444407131, 20120630))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDF = inData.toDF()
    val pPeriodDF = policyPeriod.toDF()
    LOG.info("DF Schema: {}", inDF.schema)

    // Get records from mpd_auto_driver with non_recalculated_veh_age as Null
    // join with mpd_auto_policy_period 
    // Update non_recalculated_veh_age based on original_plcy_efdt & lveh_model_yr
    val blankVehAge = col("non_recalculated_veh_age") === ""
    val diffGT99 = (col("originalPolicyEffDt").substr(0, 4) - col("lvehModelYr") + 1) > 99
    val diffLT0 = (col("originalPolicyEffDt").substr(0, 4) - col("lvehModelYr") + 1) < 0
    val UpdVehAge = (col("originalPolicyEffDt").substr(0, 4) - col("lvehModelYr") + 1).cast("Int") 

    val OutDF = inDF.join(pPeriodDF, Seq("riskStateCd", "ratingPlan", "plcyPeriodId"), "left_outer")
      .withColumn("UpdNonRecalculatedVehAge", when(blankVehAge and diffGT99, "99")
                                             .when(blankVehAge and diffLT0, "00")
                                             .when(blankVehAge, lpad(UpdVehAge.cast(StringType),2,"0"))
                                             .otherwise(col("non_recalculated_veh_age")))

    LOG.info("Out DF Schema: {}", OutDF.schema)
    OutDF.show()
    LOG.info("Ended")

  }
}

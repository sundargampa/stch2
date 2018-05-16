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
import org.apache.spark.sql.types.{ DateType, DataType, FloatType, LongType, StringType, StructType, TimestampType, IntegerType, DoubleType, BinaryType }

import java.sql.Date
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils
import com.hig.epp.utils.EppUtils

object UpdatePossibleMctdErrorInd {

  val LOG = LoggerFactory.getLogger("UpdatePossibleMctdErrorInd")

  def main(args: Array[String]) {
    LOG.info("Started")

    val mpdAutoLiability = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("possibleUnratableInd", StringType, false))

    val mpdAutoPhysicalDamage = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("possibleUnratableInd", StringType, false))

    val mpdAutoNoFault = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("possibleUnratableInd", StringType, false))

    val mpdAutoOther = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("possibleUnratableInd", StringType, false))

    val mpdAutoVehicle = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("mctdInd", StringType, true))
      .add(new StructField("possibleUnratableInd", StringType, false))

    val mpdAutoPremiumAmount = new StructType()
      .add(new StructField("riskStateCd", LongType, false))
      .add(new StructField("ratingPlan", StringType, false))
      .add(new StructField("plcyPeriodId", LongType, false))
      .add(new StructField("plcyVehicleId", LongType, false))
      .add(new StructField("coverageTypeId", IntegerType, false))
      .add(new StructField("writtenPremuim", DoubleType, true))

    val inDatampdAutoLiability = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, 1, "N"),
      Row(42L, "PLIC", 19431203L, 19431122L, 2, "N"),
      Row(42L, "PLIC", 19431211L, 19431211L, 3, "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, 1, "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, 4, "N"),
      Row(42L, "PLIC", 19431222L, 19431333L, 1, "N"),
      Row(42L, "PLIC", 19431222L, 19431344L, 2, "N"),
      Row(42L, "PLIC", 19431233L, 19431411L, 3, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, 1, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, 7, "N"),
      Row(42L, "PLIC", 19431244L, 19431522L, 1, "N"))

    val inDatampdAutoPhysicalDamage = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, 8, "N"),
      Row(42L, "PLIC", 19431203L, 19431122L, 9, "N"),
      Row(42L, "PLIC", 19431211L, 19431211L, 8, "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, 7, "N"),
      Row(42L, "PLIC", 19431222L, 19431333L, 6, "N"),
      Row(42L, "PLIC", 19431222L, 19431344L, 8, "N"),
      Row(42L, "PLIC", 19431233L, 19431411L, 9, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, 8, "N"),
      Row(42L, "PLIC", 19431244L, 19431522L, 9, "N"))

    val inDatampdAutoNoFault = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, 7, "N"),
      Row(42L, "PLIC", 19431203L, 19431122L, 7, "N"),
      Row(42L, "PLIC", 19431211L, 19431211L, 7, "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, 6, "N"),
      Row(42L, "PLIC", 19431222L, 19431333L, 7, "N"),
      Row(42L, "PLIC", 19431222L, 19431344L, 7, "N"),
      Row(42L, "PLIC", 19431233L, 19431411L, 7, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, 7, "N"),
      Row(42L, "PLIC", 19431244L, 19431522L, 7, "N"))

    val inDatampdAutoOther = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, 1, "N"),
      Row(42L, "PLIC", 19431203L, 19431122L, 2, "N"),
      Row(42L, "PLIC", 19431211L, 19431211L, 3, "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, 4, "N"),
      Row(42L, "PLIC", 19431222L, 19431333L, 1, "N"),
      Row(42L, "PLIC", 19431222L, 19431344L, 8, "N"),
      Row(42L, "PLIC", 19431233L, 19431411L, 9, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, 7, "N"),
      Row(42L, "PLIC", 19431244L, 19431522L, 7, "N"))

    val inDatampdAutoVehicle = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, "Y", "N"),
      Row(42L, "PLIC", 19431203L, 19431122L, null, "N"),
      Row(42L, "PLIC", 19431211L, 19431211L, "Y", "N"),
      Row(42L, "PLIC", 19431222L, 19431322L, null, "N"),
      Row(42L, "PLIC", 19431222L, 19431333L, "Y", "N"),
      Row(42L, "PLIC", 19431222L, 19431344L, "Y", "N"),
      Row(42L, "PLIC", 19431233L, 19431411L, null, "N"),
      Row(42L, "PLIC", 19431244L, 19431511L, "Y", "N"),
      Row(42L, "PLIC", 19431244L, 19431522L, null, "N"))

    val inDatampdAutoPremiumAmount = Seq(
      Row(42L, "PLIC", 19431203L, 19431111L, 1, "-1".toDouble),
      Row(42L, "PLIC", 19431203L, 19431122L, 1, "24.2".toDouble),
      Row(42L, "PLIC", 19431211L, 19431211L, 1, "0".toDouble),
      Row(42L, "PLIC", 19431222L, 19431322L, 7, "0".toDouble),
      Row(42L, "PLIC", 19431222L, 19431322L, 1, "0".toDouble),
      Row(42L, "PLIC", 19431222L, 19431333L, 1, "0".toDouble),
      Row(42L, "PLIC", 19431222L, 19431344L, 2, "0".toDouble),
      Row(42L, "PLIC", 19431233L, 19431411L, 1, "0".toDouble),
      Row(42L, "PLIC", 19431244L, 19431511L, 7, "0".toDouble),
      Row(42L, "PLIC", 19431244L, 19431522L, 8, "0".toDouble))

    val sparkConf = new SparkConf().setAppName("Tst").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val inDatampdAutoLiabilityrdd = sparkContext.parallelize(inDatampdAutoLiability)
    val inDatampdAutoLiabilityDF = sqlContext.createDataFrame(inDatampdAutoLiabilityrdd, mpdAutoLiability)

    val inDatampdAutoPhysicalDamagerdd = sparkContext.parallelize(inDatampdAutoPhysicalDamage)
    val inDatampdAutoPhysicalDamageDF = sqlContext.createDataFrame(inDatampdAutoPhysicalDamagerdd, mpdAutoPhysicalDamage)

    val inDatampdAutoNoFaultrdd = sparkContext.parallelize(inDatampdAutoNoFault)
    val inDatampdAutoNoFaultDF = sqlContext.createDataFrame(inDatampdAutoNoFaultrdd, mpdAutoNoFault)

    val inDatampdAutoOtherrdd = sparkContext.parallelize(inDatampdAutoOther)
    val inDatampdAutoOtherDF = sqlContext.createDataFrame(inDatampdAutoOtherrdd, mpdAutoOther)

    val inDatampdAutoVehiclerdd = sparkContext.parallelize(inDatampdAutoVehicle)
    val inDatampdAutoVehicleDF = sqlContext.createDataFrame(inDatampdAutoVehiclerdd, mpdAutoVehicle)

    val inDatampdAutoPremiumAmountrdd = sparkContext.parallelize(inDatampdAutoPremiumAmount)
    val inDatampdAutoPremiumAmountDF = sqlContext.createDataFrame(inDatampdAutoPremiumAmountrdd, mpdAutoPremiumAmount)

    val vehCohDF = inDatampdAutoLiabilityDF.filter(col("coverageTypeId").isin(Seq(1, 2, 3): _*))
      .unionAll(inDatampdAutoPhysicalDamageDF.filter(col("coverageTypeId").isin(Seq(8, 9): _*)))
      .unionAll(inDatampdAutoNoFaultDF.filter(col("coverageTypeId").isin(Seq(7): _*)))
      .unionAll(inDatampdAutoOtherDF.filter(col("coverageTypeId").isin(Seq(1, 2, 3, 7, 8, 9): _*))).dropDuplicates()
    vehCohDF.show(100)

    val bjoinVehCohmpdAutoVeh = vehCohDF.join(inDatampdAutoVehicleDF,
      vehCohDF.col("riskStateCd") === inDatampdAutoVehicleDF.col("riskStateCd")
        && vehCohDF.col("ratingPlan") === inDatampdAutoVehicleDF.col("ratingPlan")
        && vehCohDF.col("plcyPeriodId") === inDatampdAutoVehicleDF.col("plcyPeriodId")
        && vehCohDF.col("plcyVehicleId") === inDatampdAutoVehicleDF.col("plcyVehicleId")
        && inDatampdAutoVehicleDF.col("mctdInd") === "Y")
      .drop(vehCohDF.col("riskStateCd"))
      .drop(vehCohDF.col("ratingPlan"))
      .drop(vehCohDF.col("plcyPeriodId"))
      .drop(vehCohDF.col("plcyVehicleId"))
      .drop(inDatampdAutoVehicleDF.col("mctdInd"))
      .drop(vehCohDF.col("possibleUnratableInd"))

    bjoinVehCohmpdAutoVeh.show(100)

    val NonMctdVehCovDF = inDatampdAutoVehicleDF.as("d1").join(bjoinVehCohmpdAutoVeh.as("d2"),
      bjoinVehCohmpdAutoVeh.col("riskStateCd") === inDatampdAutoVehicleDF.col("riskStateCd")
        && bjoinVehCohmpdAutoVeh.col("ratingPlan") === inDatampdAutoVehicleDF.col("ratingPlan")
        && bjoinVehCohmpdAutoVeh.col("plcyPeriodId") === inDatampdAutoVehicleDF.col("plcyPeriodId")
        && col("mctdInd").isNull)
      .drop(bjoinVehCohmpdAutoVeh.col("riskStateCd"))
      .drop(bjoinVehCohmpdAutoVeh.col("ratingPlan"))
      .drop(bjoinVehCohmpdAutoVeh.col("plcyPeriodId")).select($"d1.*", $"d2.plcyVehicleId" as "mctdVeh", $"d2.coverageTypeId")

    NonMctdVehCovDF.show(100)

    val joinwithAutoPreimum = NonMctdVehCovDF.join(inDatampdAutoPremiumAmountDF,
      NonMctdVehCovDF.col("riskStateCd") === inDatampdAutoPremiumAmountDF.col("riskStateCd")
        && NonMctdVehCovDF.col("ratingPlan") === inDatampdAutoPremiumAmountDF.col("ratingPlan")
        && NonMctdVehCovDF.col("plcyPeriodId") === inDatampdAutoPremiumAmountDF.col("plcyPeriodId")
        && NonMctdVehCovDF.col("plcyVehicleId") === inDatampdAutoPremiumAmountDF.col("plcyVehicleId")
        && NonMctdVehCovDF.col("coverageTypeId") === inDatampdAutoPremiumAmountDF.col("coverageTypeId")
        && inDatampdAutoPremiumAmountDF.col("writtenPremuim") === "0".toDouble)
      .drop(inDatampdAutoPremiumAmountDF.col("riskStateCd"))
      .drop(inDatampdAutoPremiumAmountDF.col("ratingPlan"))
      .drop(inDatampdAutoPremiumAmountDF.col("plcyPeriodId"))
      .drop(inDatampdAutoPremiumAmountDF.col("plcyVehicleId"))
      .drop(inDatampdAutoPremiumAmountDF.col("coverageTypeId"))
      .drop(col("possibleUnratableInd"))

    joinwithAutoPreimum.show(200)

    /*   val updMpdAutoVehicle = inDatampdAutoVehicleDF.as("d1").join(joinwithAutoPreimum.as("d2"),
      $"d1.riskStateCd" === $"d2.riskStateCd"
        && $"d1.ratingPlan" === $"d2.ratingPlan"
        && $"d1.plcyPeriodId" === $"d2.plcyPeriodId"
        && $"d1.plcyVehicleId" === $"d2.mctdVeh",
      "left_outer")
      .drop($"d2.riskStateCd")
      .drop($"d2.ratingPlan")
      .drop($"d2.plcyPeriodId")
      .drop($"d2.plcyVehicleId")
      .drop($"d2.mctdInd")
      .withColumn("updPossUnratableInd", when(col("mctdVeh").isNull, col("possibleUnratableInd")).otherwise(lit("M")))

    updMpdAutoVehicle.show()
 */

/*    val updMpdAutoLiability = inDatampdAutoLiabilityDF.as("d1")
      .join(joinwithAutoPreimum.as("d2").filter(($"d2.coverageTypeId")
        .isin((Seq(1, 2, 3): _*))),
        $"d1.riskStateCd" === $"d2.riskStateCd"
          && $"d1.ratingPlan" === $"d2.ratingPlan"
          && $"d1.plcyPeriodId" === $"d2.plcyPeriodId"
          && $"d1.plcyVehicleId" === $"d2.mctdVeh"
          && $"d1.coverageTypeId" === $"d2.coverageTypeId",
        "left_outer")
      .drop($"d2.riskStateCd")
      .drop($"d2.ratingPlan")
      .drop($"d2.plcyPeriodId")
      .drop($"d2.plcyVehicleId")
      .drop($"d2.mctdInd")
      .withColumn("updPossUnratableInd", when(col("mctdVeh").isNull, col("possibleUnratableInd")).otherwise(lit("M")))

    updMpdAutoLiability.show()*/

  /*     val updMpdAutoPhysicalDamage = inDatampdAutoPhysicalDamageDF.as("d1")
    .join(joinwithAutoPreimum.as("d2").filter(($"d2.coverageTypeId")
        .isin((Seq(8,9): _*))),
      $"d1.riskStateCd" === $"d2.riskStateCd"
        && $"d1.ratingPlan" === $"d2.ratingPlan"
        && $"d1.plcyPeriodId" === $"d2.plcyPeriodId"
        && $"d1.plcyVehicleId" === $"d2.mctdVeh"
        && $"d1.coverageTypeId" === $"d2.coverageTypeId",
      "left_outer")
      .drop($"d2.riskStateCd")
      .drop($"d2.ratingPlan")
      .drop($"d2.plcyPeriodId")
      .drop($"d2.plcyVehicleId")
      .drop($"d2.mctdInd")
      .withColumn("updPossUnratableInd", when(col("mctdVeh").isNull, col("possibleUnratableInd")).otherwise(lit("M")))

    updMpdAutoPhysicalDamage.show()*/

/*        val updMpdAutoNoFault = inDatampdAutoNoFaultDF.as("d1")
      .join(joinwithAutoPreimum.as("d2"),
        $"d1.riskStateCd" === $"d2.riskStateCd"
          && $"d1.ratingPlan" === $"d2.ratingPlan"
          && $"d1.plcyPeriodId" === $"d2.plcyPeriodId"
          && $"d1.plcyVehicleId" === $"d2.mctdVeh"
          && $"d1.coverageTypeId" === $"d2.coverageTypeId",
        "left_outer")
      .drop($"d2.riskStateCd")
      .drop($"d2.ratingPlan")
      .drop($"d2.plcyPeriodId")
      .drop($"d2.plcyVehicleId")
      .drop($"d2.mctdInd")
      .withColumn("updPossUnratableInd", when(col("mctdVeh").isNull, col("possibleUnratableInd")).otherwise(lit("M")))

    updMpdAutoNoFault.show()*/

       val updMpdAutoOther = inDatampdAutoOtherDF.as("d1")
      .join(joinwithAutoPreimum.as("d2"),
        $"d1.riskStateCd" === $"d2.riskStateCd"
          && $"d1.ratingPlan" === $"d2.ratingPlan"
          && $"d1.plcyPeriodId" === $"d2.plcyPeriodId"
          && $"d1.plcyVehicleId" === $"d2.mctdVeh"
          && $"d1.coverageTypeId" === $"d2.coverageTypeId",
        "left_outer")
      .drop($"d2.riskStateCd")
      .drop($"d2.ratingPlan")
      .drop($"d2.plcyPeriodId")
      .drop($"d2.plcyVehicleId")
      .drop($"d2.mctdInd")
      .withColumn("updPossUnratableInd", when(col("mctdVeh").isNull, col("possibleUnratableInd")).otherwise(lit("M")))

    updMpdAutoOther.show()

    LOG.info("Ended")
  }
}

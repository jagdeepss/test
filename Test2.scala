package org.example

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.google.gson.Gson

import scala.collection.mutable

case class PlanCode(employee_id: String, planCode: String, effectiveDate: String, planType: String)
case class Keyclass(effectiveDate: String, associationType: String)

//case class PlanCode(employeed_id: String, planCode: String, effectiveDate: String, associationType: String)

case class OutPlanCode(bcPlanCode: String, bcEffectiveDate: String, bcAssociationType: String,
                       bsPlanCode: String, bsEffectiveDate: String, bsAssociationType: String,
                       effectiveRowInd: String)

object Test2 {



  def main(args: Array[String]): Unit = {
    println(".......")

    val     sc = SparkSession
      .builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.default.parallelism", "1")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    val df = Seq(
      Row("1", "C1", "1987-10-28", "C"),
      Row("1", "S2", "2013-01-01", "S"),
      Row("1", "C2", "1987-10-31", "C"),
      Row("1", "S1", "1987-10-31", "S"),
      Row("1", "C3", "2013-01-01", "C")
    )
    val someSchema = List(
      StructField("employee_id", StringType, true),
      StructField("planCode", StringType, true),
      StructField("effectiveDate", StringType, true),
      StructField("planType", StringType, true)
    )
    val someDF = sc.createDataFrame(
      sc.sparkContext.parallelize(df),
      StructType(someSchema)
    )

    import sc.implicits._

    val someDs = someDF.as[PlanCode]
   val outDf =  calcualteContractPlanCode(someDs, sc)

    outDf.show()


  }

  def calcualteContractPlanCode(df: Dataset[PlanCode], sc: SparkSession): DataFrame = {
    import sc.implicits._

    // val keybydata =  df.map(r => (new keyclass(r.effectiveDate, r.associationType), r))
    val groupedData = df.groupByKey(r => r.employee_id).mapGroups((empID, groupedValues) => {
      val sortedJoinedData = groupedValues.toList.sortBy(x => (x.effectiveDate, x.planType))
      var prevBsAssocationType = ""
      var prevBsEffectiveDate = ""
      var prevBsPlanCode = ""
      var prevBcAssocationType = ""
      var prevBcEffectiveDate = ""
      var prevBcPlanCode = ""
      val size = sortedJoinedData.size

      var finalJsonStr = ""
      var outList = new mutable.MutableList[OutPlanCode] ()

      var numLoop = 1
      for (sr <- sortedJoinedData) {
        var prev_bs_planCode_1 = ""
        var prev_bs_effectiveDate_1 = ""
        var prev_bs_associationType_1 = ""


        val outPc: OutPlanCode = if (sr.planType == "C") {
          prevBcPlanCode = sr.planCode
          prevBcEffectiveDate = sr.effectiveDate
          prevBcAssocationType = sr.planType
          if (numLoop.equals(size)) {
            new OutPlanCode(sr.planCode, sr.effectiveDate, sr.planType, prevBsPlanCode, prevBsEffectiveDate, prevBsAssocationType, "Y")
          } else {
            new OutPlanCode(sr.planCode, sr.effectiveDate, sr.planType, prevBsPlanCode, prevBsEffectiveDate, prevBsAssocationType, "N")
          }

        } else {
          prevBsPlanCode = sr.planCode
          prevBsEffectiveDate = sr.effectiveDate
          prevBsAssocationType = sr.planType
          if (numLoop.equals(size)) {
            new OutPlanCode(prevBcPlanCode, prevBcEffectiveDate, prevBcAssocationType, sr.planCode, sr.effectiveDate, sr.planType, "Y")
          } else {
            new OutPlanCode(prevBcPlanCode, prevBcEffectiveDate, prevBcAssocationType, sr.planCode, sr.effectiveDate, sr.planType, "N")
          }
        }

        outList +=  outPc
        numLoop += 1

      }
      val gson = new Gson
      val sizeOut = outList.size
      var numOutList = 1
//
//     // println("..........."+ json)
      var outStr="["

      for (o <- outList ) {
        if( numOutList.equals(sizeOut))  {
          outStr += gson.toJson(o) +"]"
        }  else {
          outStr += gson.toJson(o) +","
        }
        numOutList +=1
      }
      if (sizeOut.equals(0)) outStr=""
//println("------------"+outStr)
      (empID,outStr)
    }

    )

    groupedData.toDF("ID","jsonStr")


  }

}

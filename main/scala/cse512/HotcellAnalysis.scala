package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickupInfo")

  val filterPickupInfo = spark.sql("select x, y, z from pickupInfo where x >= " + minX + " and x <= " + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ)
  filterPickupInfo.createOrReplaceTempView("filterPickupInfo")

  val cellPntCnt = spark.sql("select x, y, z, count(*) as cnt from filterPickupInfo group by x, y, z order by count(x) desc")
  cellPntCnt.createOrReplaceTempView("cellPntCnt")

  val x_w_val = spark.sql("select cpc1.x, cpc1.y, cpc1.z, sum(cpc2.cnt) as x_val, count(cpc2.x) as w_val from cellPntCnt cpc1, cellPntCnt cpc2 where (cpc1.x = cpc2.x or cpc1.x = cpc2.x + 1 or cpc1.x = cpc2.x - 1) and (cpc1.y = cpc2.y or cpc1.y = cpc2.y + 1 or cpc1.y = cpc2.y - 1) and (cpc1.z = cpc2.z or cpc1.z = cpc2.z + 1 or cpc1.z = cpc2.z - 1) group by cpc1.x, cpc1.y, cpc1.z, cpc1.cnt")
  x_w_val.createOrReplaceTempView("x_w_val")

  val mean_sd_X = spark.sql("select sum(cnt)/"+numCells.longValue()+" as meanX, count(cnt), sqrt(sum(cnt*cnt)/"+numCells.longValue()+" - ((sum(cnt)/"+numCells.longValue()+")*sum(cnt)/"+numCells.longValue()+")) as stddev from cellPntCnt")
  mean_sd_X.createOrReplaceTempView("mean_sd_X")

  val numerator = spark.sql("select x,y,z,x_val - (select meanX from mean_sd_X)*w_val as numeratorVal from x_w_val")
  numerator.createOrReplaceTempView("numerator")

  val denominator = spark.sql("select x,y,z,(select stddev from mean_sd_X),("+numCells.longValue()+" * w_val),(w_val*w_val),"+numCells.longValue()+"-1,  (select stddev from mean_sd_X)*sqrt((("+numCells.longValue()+" * w_val) - (w_val*w_val))/("+numCells.longValue()+"-1)) as denominatorVal from x_w_val")
  denominator.createOrReplaceTempView("denominator")

  val hot_cells = spark.sql("select num.x,num.y,num.z from numerator num, denominator den where num.x = den.x and num.y = den.y and num.z = den.z order by num.numeratorVal/den.denominatorVal desc limit 50")
  var newHeader = Seq("x", "y", "z")
  val hot_cells_output = hot_cells.toDF(newHeader:_*)

  return hot_cells_output
}
}

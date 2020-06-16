// window_function - blog Code

// Import Libraries
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Create Sample DataFrame

case class Salary(depName: String, empNo: Long, salary: Long)
val empsalary = Seq(
  Salary("sales", 1, 5000),
  Salary("personnel", 2, 3900),
  Salary("sales", 3, 4800),
  Salary("sales", 4, 4800),
  Salary("personnel", 5, 3500),
  Salary("develop", 7, 4200),
  Salary("develop", 8, 6000),
  Salary("develop", 9, 4500),
  Salary("develop", 10, 5200),
  Salary("develop", 11, 5200)).toDF()

// Display DataFrame
empsalary.show()

// Create Window Specification for Aggregate Function
val byDepName = Window.partitionBy("depName")

// Apply Aggregate Function on Window
val agg_sal = (empsalary
                .withColumn("max_salary", max("salary").over(byDepName))
                .withColumn("min_salary", min("salary").over(byDepName)))
                
agg_sal.select("depname", "max_salary", "min_salary")
.dropDuplicates().show()

// Create Window Specification for Ranking Function
val winSpec = Window.partitionBy("depName").orderBy($"salary".desc)

// Apply Rank Function
val rank_df = empsalary.withColumn("rank", rank().over(winSpec))
rank_df.show()

// Apply Dense Rank Function
val dense_rank_df = empsalary.withColumn("desnse_rank", dense_rank().over(winSpec))
dense_rank_df.show()

// row_number
val row_num_df = empsalary.withColumn("row_number", row_number().over(winSpec))
row_num_df.show()

// percent_rank
val percent_rank_df = empsalary.withColumn("percent_rank", percent_rank().over(winSpec))
percent_rank_df.show()

// ntile
val ntile_df = empsalary.withColumn("ntile", ntile(3).over(winSpec))
ntile_df.show()

// cume_dist
val winSpec = Window.partitionBy("depName").orderBy("salary")
val cume_dist_df = empsalary.withColumn("cume_dist", cume_dist().over(winSpec))
cume_dist_df.show()

// lag
val winSpec = Window.partitionBy("depName").orderBy("salary")
val lag_df = empsalary.withColumn("lag", lag("salary", 2).over(winSpec))
lag_df.show()

// lead
val winSpec = Window.partitionBy("depName").orderBy("salary")
val lead_df = empsalary.withColumn("lead", lead("salary", 2).over(winSpec))
lead_df.show()

// rangeBetween
val winSpec = Window.partitionBy("depName").orderBy("salary").rangeBetween(100L, 300L)
val range_between_df = empsalary.withColumn("max_salary", max("salary").over(winSpec))
range_between_df.show()

// rangeBetween with Special boundary
/*
There are some special bourdary values which can be used here.
 Window.currentRow : to specify current value in a row.
 Window.unboundedPreceding : This can used to have unbounded start of the window.
 Window.unboundedFollowing : This can be used unbounded end of the window.
*/
val winSpec = Window.partitionBy("depName").orderBy("salary").rangeBetween(300L, Window.unboundedFollowing)
val range_unbounded_df = empsalary.withColumn("max_salary", max("salary").over(winSpec))
range_unbounded_df.show()

// rowsBetween
val winSpec = Window.partitionBy("depName").orderBy("salary").rowsBetween(-1, 1)
val rows_between_df = empsalary.withColumn("max_salary", max("salary").over(winSpec))
rows_between_df.show()

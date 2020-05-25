// Databricks notebook source
// DBTITLE 1,Load Required Libraries
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Create DataFrame
val initial_df = Seq(
  ("x", 4, 1),
  ("x", 6, 2),
  ("z", 7, 3),
  ("a", 3, 4),
  ("z", 5, 2),
  ("x", 7, 3),
  ("x", 9, 7),
  ("z", 1, 8),
  ("z", 4, 9),
  ("z", 7, 4),
  ("a", 8, 5),
  ("a", 5, 2),
  ("a", 3, 8),
  ("x", 2, 7),
  ("z", 1, 9)
).toDF("col1", "col2", "col3")

initial_df.show()

// COMMAND ----------

// DBTITLE 1,collect_list
val full_df = initial_df.groupBy("col1")
               .agg(collect_list($"col2").as("array_col1"),
                    collect_list($"col3").as("array_col2"))

val df = full_df.drop("array_col1")

// COMMAND ----------

// DBTITLE 1,Check Schema
full_df.printSchema()

// COMMAND ----------

// DBTITLE 1,array_contains
// Returns null if the array is null, true if the array contains value, and false otherwise.
val arr_contains_df = df.withColumn("result", array_contains($"array_col2", 3))
arr_contains_df.show()

// COMMAND ----------

// DBTITLE 1,array_distinct
// Removes duplicate values from the array.
val arr_distinct_df = df.withColumn("result", array_distinct($"array_col2"))
arr_distinct_df.show()

// COMMAND ----------

// DBTITLE 1,array_except
// Returns an array of the elements in the first array but not in the second array, without duplicates. The order of elements in the result is not determined
val arr_except_df = full_df.withColumn("result", array_except($"array_col1", $"array_col2"))
arr_except_df.show()

// COMMAND ----------

// DBTITLE 1,array_intersect
// Returns an array of the elements in the intersection of the given two arrays, without duplicates.
val arr_intersect_df = full_df.withColumn("result", array_intersect($"array_col1", $"array_col2"))
arr_intersect_df.show()

// COMMAND ----------

// DBTITLE 1,array_join
// Concatenates the elements of column using the delimiter.
// Returns an array of the elements in the intersection of the given two arrays, without duplicates.
val arr_join_df = df.withColumn("result", array_join($"array_col2", ","))
arr_join_df.show()
// if there are any null values then we can replace with third argument ("nullReplacement") with any string value.

// COMMAND ----------

// DBTITLE 1,array_max
// Returns the maximum value in the array.
val arr_max_df = df.withColumn("result", array_max($"array_col2"))
arr_max_df.show()

// COMMAND ----------

// DBTITLE 1,array_min
// Returns the minimum value in the array.
val arr_min_df = df.withColumn("result", array_min($"array_col2"))
arr_min_df.show()

// COMMAND ----------

// DBTITLE 1,array_position
// Locates the position of the first occurrence of the value in the given array as long. Returns null if either of the arguments are null.
// return 0 is the value is not present

val arr_pos_df = df.withColumn("result", array_position($"array_col2", 7))
arr_pos_df.show()

// COMMAND ----------

// DBTITLE 1,array_remove
// Remove all elements that equal to element from the given array.

val arr_remove_df = df.withColumn("result", array_remove($"array_col2", 7))
arr_remove_df.show()

// Now we can see all occurance of element "7" is removed from all arrays.

// COMMAND ----------

// DBTITLE 1,array_repeat
// Creates an array containing the left argument repeated the number of times given by the right argument.

val arr_repeat_df = df.withColumn("result", array_repeat($"array_col2", 2))
arr_repeat_df.show(truncate = false)

// truncate = false will not truncate the column values during display.

// COMMAND ----------

// DBTITLE 1,array_sort
// Sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.

val arr_sort_df = df.withColumn("result", array_sort($"array_col2"))
arr_sort_df.show()

// COMMAND ----------

// DBTITLE 1,array_union
// Returns an array of the elements in the union of the given two arrays, without duplicates.

val arr_union_df = full_df.withColumn("result", array_union($"array_col1", $"array_col2")).drop("col1")
arr_union_df.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,arrays_overlap
// Returns true if a1 and a2 have at least one non-null element in common. If not and both the arrays are non-empty and any of them contains a null, it returns null. It returns false otherwise.

val arr_overlap_df = full_df.withColumn("result", arrays_overlap($"array_col1", $"array_col2"))
arr_overlap_df.show()

// COMMAND ----------

// DBTITLE 1,arrays_zip
// Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
// Since our both the array columns have same numbers of values, let remove some values from one array column and see how it behaves with differenet values in array with zip operation.

val temp_df = full_df.withColumn("new_array_col", array_remove($"array_col2",2))
val arr_zip_df = temp_df
                  .withColumn("result", arrays_zip($"array_col1", $"new_array_col"))
                  .select("array_col1", "new_array_col", "result")
arr_zip_df.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,Check Schema
arr_zip_df.select("result").printSchema()

// COMMAND ----------

// DBTITLE 1,concat
// Concatenates multiple input columns together into a single column.

val arr_cat_df = full_df.withColumn("result", concat($"array_col1", $"array_col2")).drop("col1")
arr_cat_df.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,element_at
// Returns element of array at given index in value if column is array. Returns value for the given key in value if column is map.

val arr_element_at_df = df.withColumn("result", element_at($"array_col2", 1))
arr_element_at_df.show()

// COMMAND ----------

// DBTITLE 1,flatten
// Creates a single array from an array of arrays. If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.

val arr_repeat_df = df.withColumn("repeat", array_repeat($"array_col2", 2))
val arr_flat_df = arr_repeat_df.withColumn("result", flatten($"repeat")).select("repeat", "result")
arr_flat_df.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,map_from_arrays
// Creates a new map column. The array in the first column is used for keys. The array in the second column is used for values. All elements in the array for key should not be null.

val map_from_arr_df = full_df.withColumn("result", map_from_arrays($"array_col1", $"array_col2")).drop("col1")
map_from_arr_df.show()

// COMMAND ----------

// DBTITLE 1,reverse
// Returns a reversed string or an array with reverse order of elements.

val arr_reverse_df = df.withColumn("result", reverse($"array_col2"))
arr_reverse_df.show()

// COMMAND ----------

// DBTITLE 1,shuffle
// Returns a random permutation of the given array.

val arr_shuffle_df = df.withColumn("result", shuffle($"array_col2"))
arr_shuffle_df.show()

// Execute the shuffle function miltiple times and observer the "result". Order of values in "result" column will be different for each execution. 

// COMMAND ----------

// DBTITLE 1,size
// Returns length of array or map.

val arr_size_df = df.withColumn("result", size($"array_col2"))
arr_size_df.show()

// COMMAND ----------

// DBTITLE 1,slice
// Returns an array containing all the elements in x from index start (or starting from the end if start is negative) with the specified length.

val arr_slice_df = df.withColumn("result", slice($"array_col2", 2, 3))
arr_slice_df.show()

// COMMAND ----------

// DBTITLE 1,sort_array
// Sorts the input array for the given column in ascending or descending order, according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.

val arr_sort_df = df.withColumn("result", sort_array($"array_col2", asc=false))
arr_sort_df.show()

// default value of asc is true.

// COMMAND ----------

// DBTITLE 1,Slice array to explode
// create an array column with few values to explode.
val temp_df = df.withColumn("slice_col", slice($"array_col2", 1, 2)).drop("array_col2")
temp_df.show()

// COMMAND ----------

// DBTITLE 1,explode
// Creates a new row for each element in the given array or map column.

// explode the array column "slice_col"
val arr_explode_df = temp_df.withColumn("result", explode($"slice_col"))
arr_explode_df.show()

// COMMAND ----------

// DBTITLE 1,posexplode
// Creates a new row for each element with position in the given array or map column.

// create an array column with few values to explode.
val temp_df = df.withColumn("slice_col", slice($"array_col2", 1, 2)).drop("array_col2")

// explode the array column "slice_col"
val arr_explode_df = temp_df.select($"*", posexplode($"slice_col"))
arr_explode_df.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,Reference
// MAGIC %md
// MAGIC - https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$

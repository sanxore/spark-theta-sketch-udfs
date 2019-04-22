# Spark sql Theta Sketch UDFs

This project is made to solve the count-distinct problem for billions of identifiers in distribute way using spark and ThetaSketch.

It contains three UDAFs and a UDF:

- UDAFs:
    - `DataToSketch` transform sets of identifiers to sketches
    - `UnionSketch` apply unions set operation between sketches, the output is a sketch
    - `IntersectSketch` apply intersection set operation between sketches

- UDF:
    - `estimate` estimate the cardinality, the input should be a sketch and the output is a float


## Workload performance

In this tables we compare speed and accuracy performance between exact and approximate count-distinct in various workloads

| Input volume  | count-distinct | approximation | accuracy |
| --------------|:--------------:|:-------------:|:--------:|
| 10000         | ~              | ~~            | ~~~      |
| 100000        | ~              | ~~            | ~~~      |
| 1000000       | ~              | ~~            | ~~~      |



| Input volume  | count-distinct (time) | pre-processing sketch (time) | approximation (time) |
| --------------|:---------------------:|:----------------------------:|:--------------------:|
| 10000         | ~                     | ~~                           | ~~~                  |
| 100000        | ~                     | ~~                           | ~~~                  |
| 1000000       | ~                     | ~~                           | ~~~                  |


## Literature

- [ThetaSketch Framework](https://datasketches.github.io/docs/Theta/ThetaSketchFramework.html)
- [Java example Theta sketch](https://datasketches.github.io/docs/Theta/ThetaJavaExample.html)
- [Spark UDAF doc](https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html)


## Install

Note: The assembly file will contain just the Yahoo `sketches-core` and `memory` package

```bash
$ sbt assembly
```

### Load to spark

```bash
$ spark-shell --jars {path_to_jar}/sketches-spark-assembly-0.1.jar
```

### Import and load UDAFs and UDF

```scala
import org.apache.spark.sql.functions.udf

import com.sketches.spark.theta.udaf.{DataToSketchUDAF, IntersectSketchUDAF, UnionSketchUDAF}
import com.sketches.spark.theta.functions.estimate

spark.udf.register("data_sketch", new DataToSketchUDAF)
spark.udf.register("union_sketch", new UnionSketchUDAF)
spark.udf.register("inter_sketch", new IntersectSketchUDAF)
val estimate_udf = udf((x: Array[Byte]) => estimate(x))
spark.udf.register("estimate", estimate_udf)
```

## Usage:

```scala
import org.apache.spark.sql.functions.udf

import com.sketches.spark.theta.udaf.{DataToSketchUDAF, IntersectSketchUDAF, UnionSketchUDAF}
import com.sketches.spark.theta.functions.estimate

// register sketching UDFs
spark.udf.register("data_sketch", new DataToSketchUDAF)
spark.udf.register("union_sketch", new UnionSketchUDAF)
spark.udf.register("inter_sketch", new IntersectSketchUDAF)
val estimate_udf = udf((x: Array[Byte]) => estimate(x))
spark.udf.register("estimate", estimate_udf)

// generate sample data
val ids = spark.range(1, 20)
ids.registerTempTable("ids")
val df = spark.sql("select id, id % 3 as group_id from ids")
df.registerTempTable("simple")

spark.sql("SELECT estimate(union_sketch(sketch)) FROM (SELECT id, data_sketch(group_id) AS sketch FROM simple GROUP BY id) t").show()
spark.sql("SELECT estimate(union_sketch(sketch)) FROM (SELECT group_id, data_sketch(id) AS sketch FROM simple GROUP BY group_id) t").show()
```

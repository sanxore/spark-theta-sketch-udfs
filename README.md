# Spark sql Theta Sketch UDFs

## Assembly

Note: The assembly file will contain just the Yahoo `sketches-core` and `memory` package

```bash
$ sbt assembly
```

## import to spark

```bash
$ spark-shell --jars {path_to_jar}/sketches-spark-assembly-0.1.jar
```

## Example:

```scala
import org.apache.spark.sql.functions.udf

import com.sketches.spark.theta.udaf.{DataToSketchUDAF, IntersectSketchUDAF, UnionSketchUDAF}
import com.sketches.spark.theta.functions.estimate

// generate sample data
val ids = spark.range(1, 20)
ids.registerTempTable("ids")
val df = spark.sql("select id, id % 3 as group_id from ids")
df.registerTempTable("simple")

// register sketching UDFs
spark.udf.register("data_sketch", new DataToSketchUDAF)
spark.udf.register("union_sketch", new UnionSketchUDAF)
spark.udf.register("inter_sketch", new IntersectSketchUDAF)
val estimate_udf = udf((x: Array[Byte]) => estimate(x))
spark.udf.register("estimate", estimate_udf)

spark.sql("SELECT estimate(union_sketch(sketch)) FROM (SELECT id, data_sketch(group_id) AS sketch FROM simple GROUP BY id) t").show()
spark.sql("SELECT estimate(union_sketch(sketch)) FROM (SELECT group_id, data_sketch(id) AS sketch FROM simple GROUP BY group_id) t").show()
```

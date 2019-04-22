package com.sketches.spark.theta.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.yahoo.sketches.theta.{Sketch, UpdateSketch, SetOperation, Union}
import com.yahoo.memory.{Memory, WritableMemory}


class DataToSketchUDAF(k:Int=4096) extends UserDefinedAggregateFunction {

  override def inputSchema: StructType =
  /* todo can't cast numeric types to binary maybe a spark issue*/
    StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("update_sketch", BinaryType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val value = input.getAs[String](0)
    if (buffer(0) == null) {
      val sketch = UpdateSketch.builder.setNominalEntries(k).build
      sketch.update(value)
      buffer(0) = sketch.toByteArray
    }

    else {
      val memorySketch = WritableMemory.wrap(buffer.getAs[Array[Byte]](0))
      val sketch = UpdateSketch.wrap(memorySketch)
      sketch.update(value)
      buffer(0) = sketch.toByteArray
    }


  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }

    else if (buffer1(0) != null && buffer2(0) != null){
      val memorySketch1 = WritableMemory.wrap(buffer1.getAs[Array[Byte]](0))
      val memorySketch2 = WritableMemory.wrap(buffer2.getAs[Array[Byte]](0))

      val sketch1 = Sketch.wrap(memorySketch1)
      val sketch2 = Sketch.wrap(memorySketch2)

      val union: Union = SetOperation.builder.setNominalEntries(k).buildUnion
      union.update(sketch1)
      union.update(sketch2)
      buffer1(0) = union.getResult.toByteArray
    }
  }

  override def evaluate(buffer: Row): Any = {
    val memorySketch = Memory.wrap(buffer.getAs[Array[Byte]](0))
    val sketch = Sketch.wrap(memorySketch)
    sketch.toByteArray
  }
}

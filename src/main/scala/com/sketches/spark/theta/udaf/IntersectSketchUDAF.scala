package com.sketches.spark.theta.udaf

import com.yahoo.memory.{Memory, WritableMemory}
import com.yahoo.sketches.theta.{Intersection, SetOperation, Sketch}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{BinaryType, DataType, StructField, StructType}

class IntersectSketchUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType =
    StructType(StructField("sketch", BinaryType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("intersect_sketch", BinaryType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputMemorySketch: Memory = Memory.wrap(input.getAs[Array[Byte]](0))
    /*todo can be avoid by adding update from Memory object in Intersection*/
    val inputSketch = Sketch.wrap(inputMemorySketch)

    if (buffer(0) == null) {
      val intersectSketch: Intersection = SetOperation.builder.buildIntersection
      intersectSketch.update(inputSketch)
      buffer(0) = intersectSketch.toByteArray
    }

    else {
      val intersectionMemorySketch = WritableMemory.wrap(buffer.getAs[Array[Byte]](0))
      val intersectionSketch = SetOperation.wrap(intersectionMemorySketch).asInstanceOf[Intersection]
      intersectionSketch.update(inputSketch)
      buffer(0) = intersectionSketch.toByteArray
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }

    else if (buffer1(0) != null && buffer2(0) != null) {
      val memorySketch1 = Memory.wrap(buffer1.getAs[Array[Byte]](0))
      val memorySketch2 = Memory.wrap(buffer2.getAs[Array[Byte]](0))

      val sketch1 = SetOperation.wrap(memorySketch1).asInstanceOf[Intersection].getResult
      val sketch2 = SetOperation.wrap(memorySketch2).asInstanceOf[Intersection].getResult

      val intersectionSketch: Intersection = SetOperation.builder.buildIntersection
      intersectionSketch.update(sketch1)
      intersectionSketch.update(sketch2)
      buffer1(0) = intersectionSketch.toByteArray
    }
  }

  override def evaluate(buffer: Row): Any = {
    val memorySketch = Memory.wrap(buffer.getAs[Array[Byte]](0))
    val sketch = SetOperation.wrap(memorySketch).asInstanceOf[Intersection].getResult
    sketch.toByteArray
  }
}

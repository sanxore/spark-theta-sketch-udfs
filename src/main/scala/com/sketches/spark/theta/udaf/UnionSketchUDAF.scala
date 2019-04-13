package com.sketches.spark.theta.udaf

import com.yahoo.memory.{Memory, WritableMemory}
import com.yahoo.sketches.theta.{SetOperation, Union}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class UnionSketchUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType =
    StructType(StructField("sketch", BinaryType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("union_sketch", BinaryType) :: Nil)

  override def dataType: DataType = BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val InputMemorySketch: Memory = Memory.wrap(input.getAs[Array[Byte]](0))

    if (buffer(0) == null) {
      val unionSketch: Union = SetOperation.builder.buildUnion
      unionSketch.update(InputMemorySketch)
      buffer(0) = unionSketch.toByteArray
    }

    else {
      val unionMemorySketch = WritableMemory.wrap(buffer.getAs[Array[Byte]](0))
      val unionSketch = SetOperation.wrap(unionMemorySketch).asInstanceOf[Union]
      unionSketch.update(InputMemorySketch)
      buffer(0) = unionSketch.toByteArray
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1(0) == null) {
      buffer1(0) = buffer2(0)
    }

    else if (buffer1(0) != null && buffer2(0) != null){
      val memorySketch1 = WritableMemory.wrap(buffer1.getAs[Array[Byte]](0))
      val memorySketch2 = WritableMemory.wrap(buffer2.getAs[Array[Byte]](0))

      val sketch1 = SetOperation.wrap(memorySketch1).asInstanceOf[Union].getResult
      val sketch2 = SetOperation.wrap(memorySketch2).asInstanceOf[Union].getResult

      val union: Union = SetOperation.builder.buildUnion
      union.update(sketch1)
      union.update(sketch2)
      buffer1(0) = union.toByteArray
    }
  }

  override def evaluate(buffer: Row): Any = {
    val memorySketch = Memory.wrap(buffer.getAs[Array[Byte]](0))
    val sketch = SetOperation.wrap(memorySketch).asInstanceOf[Union].getResult
    sketch.toByteArray
  }
}

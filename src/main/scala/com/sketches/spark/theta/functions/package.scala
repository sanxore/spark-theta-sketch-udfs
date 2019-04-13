package com.sketches.spark.theta

import com.yahoo.memory.Memory
import com.yahoo.sketches.theta.Sketch

package object functions {

  def estimate(sketchBytes: Array[Byte]): Double = {
    val memorySketch = Memory.wrap(sketchBytes)
    val sketch = Sketch.wrap(memorySketch)
    sketch.getEstimate
  }

}

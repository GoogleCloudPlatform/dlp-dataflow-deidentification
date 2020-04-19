package com.google.swarm.tokenization.common;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteValueCombineFn extends CombineFn<Long, ByteValueCombineFn.Accum, Long> {
  public static final Logger LOG = LoggerFactory.getLogger(ByteValueCombineFn.class);

  public static class Accum implements Serializable {
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((inspectBytes == null) ? 0 : inspectBytes.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Accum other = (Accum) obj;
      if (inspectBytes == null) {
        if (other.inspectBytes != null) return false;
      } else if (!inspectBytes.equals(other.inspectBytes)) return false;
      return true;
    }

    Long inspectBytes = 0L;
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, Long input) {

    mutableAccumulator.inspectBytes += input;
    return mutableAccumulator;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    Accum merged = createAccumulator();
    for (Accum accum : accumulators) {
      merged.inspectBytes = accum.inspectBytes;
    }
    return merged;
  }

  @Override
  public Long extractOutput(Accum accumulator) {
    return accumulator.inspectBytes;
  }
}

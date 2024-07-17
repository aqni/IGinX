/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.parquet.util.arrow;

import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;

import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.VectorAppender;

public class TransferVectorAppender extends VectorAppender {

  protected final ValueVector targetVector;

  public TransferVectorAppender(ValueVector targetVector) {
    super(targetVector);
    this.targetVector = targetVector;
  }

  @Override
  public ValueVector visit(BaseVariableWidthViewVector deltaVector, Void value) {
    Preconditions.checkArgument(
        targetVector.getField().getType().equals(deltaVector.getField().getType()),
        "The targetVector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // nothing to append, return
    }

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetViewSize = targetVector.getValueCount() * BaseVariableWidthViewVector.ELEMENT_SIZE;
    int deltaViewSize = deltaVector.getValueCount() * BaseVariableWidthViewVector.ELEMENT_SIZE;
    int newViewSize = targetViewSize + deltaViewSize;

    // make sure there is enough capacity
    while (capAtMaxInt(targetVector.getValidityBuffer().capacity() * 8) < newValueCount) {
      ((BaseVariableWidthViewVector) targetVector).reallocValidityBuffer();
    }
    while (capAtMaxInt(
            targetVector.getDataBuffer().capacity() / BaseVariableWidthViewVector.ELEMENT_SIZE)
        < newValueCount) {
      ((BaseVariableWidthViewVector) targetVector).reallocViewBuffer();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
        targetVector.getValidityBuffer(),
        targetVector.getValueCount(),
        deltaVector.getValidityBuffer(),
        deltaVector.getValueCount(),
        targetVector.getValidityBuffer());

    // append view buffer
    MemoryUtil.UNSAFE.copyMemory(
        deltaVector.getDataBuffer().memoryAddress(),
        targetVector.getDataBuffer().memoryAddress() + targetViewSize,
        deltaViewSize);

    // transfer data buffers
    List<ArrowBuf> targetDataBuffers =
        ((BaseVariableWidthViewVector) targetVector).getDataBuffers();
    List<ArrowBuf> deltaDataBuffers = deltaVector.getDataBuffers();

    int oldTargetDataBuffers = targetDataBuffers.size();

    deltaDataBuffers.forEach(buf -> buf.getReferenceManager().retain());
    targetDataBuffers.addAll(deltaDataBuffers);

    // update each buf.index from the second buffer
    if (oldTargetDataBuffers > 0) {
      for (int i = 0; i < deltaVector.getValueCount(); i++) {
        int index = targetVector.getValueCount() + i;
        int valueLength = ((BaseVariableWidthViewVector) targetVector).getValueLength(index);

        if (valueLength > BaseVariableWidthViewVector.INLINE_SIZE) {
          long indexOfBufferIndex =
              ((long) index * BaseVariableWidthViewVector.ELEMENT_SIZE)
                  + BaseVariableWidthViewVector.LENGTH_WIDTH
                  + BaseVariableWidthViewVector.PREFIX_WIDTH;
          int oldBufferIndex = targetVector.getDataBuffer().getInt((indexOfBufferIndex));
          targetVector
              .getDataBuffer()
              .setInt(indexOfBufferIndex, oldBufferIndex + oldTargetDataBuffers);
        }
      }
    }

    ((BaseVariableWidthViewVector) targetVector).setLastSet(newValueCount - 1);
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }
}

package io.paradoxical.carlyle.core.db.packing

object BitGroup {
  def zero(max: Int): BitGroup = {
    val bytesRequired = max / 8

    new BitGroup(new Array[Byte](bytesRequired + 1), max)
  }

  def filled(max: Int): BitGroup = {
    val bytesRequired = max / 8

    val default = 0xFF

    val array = new Array[Byte](bytesRequired + 1)

    array.indices.foreach(array.update(_, default.toByte))

    val unusedBits = (bytesRequired + 1) * 8 - max

    val lastMax = (default >> unusedBits).toByte

    // mask off the last unused bits to be 0
    array.update(array.length - 1, lastMax)

    new BitGroup(array, max)
  }
}

object Masker {
  /**
   * Creates a bitmask for a long that sets the values in the mask to 0
   *
   * For example, setting 1, 4 would make a bitmask of
   *
   * 0b .... 1111110110
   *
   * Keeping all bits the same except for bits 1 and 4 flipped to zero
   *
   * @param values
   * @return
   */
  def toggleZero(values: Iterable[Int]): Array[Byte] = {
    BitGroup.filled(64).setValues(values.toList, Bit.Zero).data
  }
}

class BitGroup(val data: Array[Byte], max: Int) {
  def count: Long = {
    data.foldLeft(0)((count, byte) => count + setInByte(byte))
  }

  private def setInByte(byte: Byte): Int = {
    (0 until 8).map(valueAt(byte, _)).count(_ == Bit.One)
  }

  def setValues(values: List[Int], bit: Bit): BitGroup = {
    require(values.forall(_ <= max), s"Cannot set values above max $max")

    val updatedData =
      values.foldLeft(data)((bytes, v) => {
        val position = byteAt(v)

        val updatedByte =
          (bit match {
            case Bit.One =>
              val mask = 1 << position.bitPosition

              mask | position.byte
            case Bit.Zero =>
              val mask = ~(1 << position.bitPosition)

              mask & position.byte
          }).toByte

        bytes.update(position.bytePosition, updatedByte)

        bytes
      })

    new BitGroup(updatedData, max)
  }

  def valueAt(idx: Int): Bit = {
    val position = byteAt(idx)

    valueAt(position.byte, position.bitPosition)
  }

  private def valueAt(byte: Byte, bitPosition: Int): Bit = {
    (byte >> bitPosition) & 1 match {
      case 0 => Bit.Zero
      case 1 => Bit.One
      case _ => throw new RuntimeException("Never should have a non binary value for a bit!")
    }
  }

  private def byteAt(idx: Int): BytePosition = {
    val bytePosition = idx / 8

    val byte = data(bytePosition)

    val bitPosition = idx % 8

    BytePosition(byte, bytePosition, bitPosition)
  }
}

case class BytePosition(byte: Byte, bytePosition: Int, bitPosition: Int)

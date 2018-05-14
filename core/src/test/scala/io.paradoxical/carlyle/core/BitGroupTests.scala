package io.paradoxical.carlyle.core

import io.paradoxical.carlyle.core.db.packing.{Bit, BitGroup}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class BitGroupTests extends FlatSpec with Matchers with PropertyChecks {
  "BitGroup" should "set values" in {
    val max = 100

    val group = BitGroup.zero(max)

    (0 until max).map(group.valueAt).forall(_ == Bit.Zero)

    val updated = group.setValues(List(57), Bit.One)

    assert(updated.valueAt(57) == Bit.One)

    assert(updated.setValues(List(57), Bit.Zero).valueAt(57) == Bit.Zero)
  }

  it should "treat 0 as an item" in {
    val group = BitGroup.zero(100)

    val updated = group.setValues(List(0), Bit.One)

    assert(updated.valueAt(0) == Bit.One)
  }

  it should "set ones" in {
    val max = 10000

    implicit def arbInterval: Arbitrary[Int] = Arbitrary(Gen.choose(0, max - 1))

    forAll { (data: Set[Int]) =>

      val items = data.filter(_ >= 0)

      val zero = BitGroup.zero(max)

      assert(zero.count == 0)

      val group = zero.setValues(items.toList, Bit.One)

      assert(items.map(group.valueAt).forall(_ == Bit.One))

      assert(group.count == items.size)
    }
  }

  it should "set zeros" in {
    val max = 10000

    implicit def arbInterval: Arbitrary[Int] = Arbitrary(Gen.choose(0, max - 1))

    forAll { (data: Set[Int]) =>

      val items = data.filter(_ >= 0)

      val zero = BitGroup.filled(max)

      assert(zero.count == max)

      val group = zero.setValues(items.toList, Bit.Zero)

      assert(items.map(group.valueAt).forall(_ == Bit.Zero))

      assert((max - group.count) == items.size)
    }
  }
}

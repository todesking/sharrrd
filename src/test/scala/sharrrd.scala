package com.todesking.sharrrd

import org.specs2.mutable._
import org.specs2.specification.Scope

class SharrrdSpec extends Specification {
  case class TestKey(id:Int)
  case class TestNode(id:Int)

  def testHash(key:TestKey):Int = key.id % 3

  class TestHashRing(mapping:Map[Int, TestNode]) extends HashRing[Int, TestNode] {
    def this(mapping:(Int, Int)*) = this(mapping.map{case (h, n) => (h, TestNode(n))}.toMap)
    override def realNodeOf(hash:Int):TestNode = mapping(hash)
  }

  def newSharding(current:HashRing[Int, TestNode], old:Seq[HashRing[Int, TestNode]]) =
    new Sharding[TestKey, Int, TestNode]({key => key.id % 3}, current, old)

  "Sharding with no history" should {
    class ctx extends Scope {
      val subject = newSharding(
        new TestHashRing(0 -> 0, 1 -> 10, 2 -> 20),
        Seq()
      )
    }

    "determine current node from key" in new ctx {
      // they have same hash value(1)
      subject.realNodeOf(TestKey(1)) === TestNode(10)
      subject.realNodeOf(TestKey(4)) === TestNode(10)

      subject.realNodeOf(TestKey(0)) === TestNode(0)
    }

    "operate() with current NodeMap" in new ctx {
      subject.operate(TestKey(1)){ node => node } === TestNode(10)
    }
  }

  "Sharding with some history" should {
    class ctx extends Scope {
      val subject = newSharding(
        new TestHashRing(0 -> 0, 1 -> 10, 2 -> 20),
        Seq(
          // same as current
          new TestHashRing(0 -> 0, 1 -> 10, 2 -> 20),
          new TestHashRing(0 -> 20, 1 -> 0, 2 -> 10)
        )
      )
    }
    "operate() with current NodeMap" in new ctx {
      subject.operate(TestKey(1)){ node => node } === TestNode(10)
    }

    "operateUntil() to traverse history" in new ctx {
      def f(nm:HashRing[Int, TestNode], rn:TestNode) = if(rn.id == 0) Some(1) else None
      subject.operateUntil(TestKey(1), 0)(f) === None
      subject.operateUntil(TestKey(1), 1)(f) === None
      subject.operateUntil(TestKey(1), 2)(f) === Some(1)
      subject.operateUntil(TestKey(1), 100)(f) === Some(1)
    }
  }
}

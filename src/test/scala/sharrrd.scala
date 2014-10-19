package com.todesking.sharrrd

import org.specs2.mutable._
import org.specs2.specification.Scope

class SharrrdSpec extends Specification {
  case class TestKey(id:Int)
  case class TestNode(id:Int)

  class TestSharding(
    override val currentNodeMap:NodeMap[TestNode],
    override val nodeMapHistory:Seq[NodeMap[TestNode]],
    override val hashRing:HashRing[TestKey, Long]
  ) extends Sharding[TestKey, Long, TestNode]

  class TestNodeMap(mapping:Map[VirtualNode, TestNode]) extends NodeMap.DefaultImpl[TestNode](mapping) {
    def this(mapping:(Int, Int)*) =
      this(mapping.map {
        case (vn, rn) => VirtualNode(vn) -> TestNode(rn)
      }.toMap)
  }

  class TestHashRing(mapping:(Long, VirtualNode)*) extends HashRing.DefaultImpl[TestKey, Long] {
    override def hashOf(key:TestKey):Long = key.id % 3
    override val table:collection.immutable.SortedMap[Long, VirtualNode] = {
      mapping.foldLeft(
        collection.immutable.TreeMap.empty[Long, VirtualNode]
      ) {(a, x) => a + x}
    }
  }

  "Sharding with no history" should {
    class ctx extends Scope {
      val subject = new TestSharding(
        new TestNodeMap(0 -> 0, 1 -> 10, 2 -> 20),
        Seq(),
        new TestHashRing(
          0L -> VirtualNode(0),
          1L -> VirtualNode(1),
          3L -> VirtualNode(2)
        )
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
      val subject = new TestSharding(
        new TestNodeMap(0 -> 0, 1 -> 10, 2 -> 20),
        Seq(
          // same as current
          new TestNodeMap(0 -> 0, 1 -> 10, 2 -> 20),
          new TestNodeMap(0 -> 20, 1 -> 0, 2 -> 10)
        ),
        new TestHashRing(
          0L -> VirtualNode(0),
          1L -> VirtualNode(1),
          3L -> VirtualNode(2)
        )
      )
    }
    "operate() with current NodeMap" in new ctx {
      subject.operate(TestKey(1)){ node => node } === TestNode(10)
    }

    "operateUntil() to traverse history" in new ctx {
      def f(nm:NodeMap[TestNode], rn:TestNode) = if(rn.id == 0) Some(1) else None
      subject.operateUntil(TestKey(1), 0)(f) === None
      subject.operateUntil(TestKey(1), 1)(f) === None
      subject.operateUntil(TestKey(1), 2)(f) === Some(1)
      subject.operateUntil(TestKey(1), 100)(f) === Some(1)
    }
  }
}

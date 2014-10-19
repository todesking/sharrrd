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
  ) extends Sharding[TestKey, Long, TestNode] {
  }
  class TestNodeMap(mapping:Map[VirtualNode, TestNode]) extends NodeMap.DefaultImpl[TestNode](mapping)
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
        new TestNodeMap(Map(0 -> 0, 1 -> 10, 2 -> 20).map{
          case (vn, rn) => VirtualNode(vn) -> TestNode(rn)
        }),
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
}

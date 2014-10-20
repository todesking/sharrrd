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

  "CloneableRandom" should {
    "clone its current state" in {
      val r1 = new HashRing.CloneableRandom(0L)

      (1 to 10).foreach { _ => r1.nextInt() }

      val r2 = r1.clone()

      (1 to 10).map{_ => r1.nextInt()} must_== (1 to 10).map{_ => r2.nextInt()}
    }
  }

  "default AssignmentPolicy" should {
    "assign hash value for node" in {
      val p = new HashRing.DefaultAssignmentPolicy[TestNode](100, new HashRing.CloneableRandom(0L))

      val assignResult = p.newAssigner().assign(Set(), TestNode(1))

      assignResult must have size(100)

      p.newAssigner().assign(Set(), TestNode(1)) must_== assignResult

      p.newAssigner().assign(assignResult.toSet, TestNode(1)) must_!= assignResult
    }
  }

  "default HashRing" should {
    val policy = new HashRing.DefaultAssignmentPolicy[TestNode](100, new HashRing.CloneableRandom(0L))

    "lookup node from hash when only one registered node" in {
      val hr = new HashRing.DefaultImpl(collection.immutable.TreeMap(1 -> TestNode(1)), policy)
      hr.realNodeOf(1000) must_== TestNode(1)
      hr.realNodeOf(-1000) must_== TestNode(1)
    }

    "lookup node from hash when more registered nodes" in {
      val hr = new HashRing.DefaultImpl(collection.immutable.TreeMap(1 -> TestNode(1), 10 -> TestNode(10)), policy)
      hr.realNodeOf(1) must_== TestNode(1)
      hr.realNodeOf(2) must_== TestNode(10)
      hr.realNodeOf(10) must_== TestNode(10)
      hr.realNodeOf(11) must_== TestNode(1)
      hr.realNodeOf(99) must_== TestNode(1)
    }

    "add and remove node" in {
      val hr = new HashRing.DefaultImpl(policy)

      val hr2 = hr.add(Seq(TestNode(99)))

      hr2.realNodeOf(1) must_== TestNode(99)

      val hr3 = hr2
        .add(Seq(TestNode(1)))
        .remove(Seq(TestNode(1), TestNode(99)))
        .add(Seq(TestNode(33)))

      hr3.realNodeOf(100) must_== TestNode(33)
    }

    "have permissive distribution" in {
      val hr = new HashRing.DefaultImpl(policy).add(Seq(TestNode(1), TestNode(2), TestNode(3)))

      val stat = (-10000 to 10000).view.map {i:Int => hr.realNodeOf(i * 100000).id}.foldLeft(Map.empty[Int, Int]){(a:Map[Int, Int], x:Int) =>
        a + (x -> ((a.get(x) getOrElse 0) + 1))
      }

      stat.size must_== 3

      val sum = stat.values.sum
      val avg = sum / stat.size.toDouble
      val maxDiff = avg * 0.1

      stat.values.foreach { v =>
        println(v)
        Math.abs(avg - v) must be_<(maxDiff)
      }
      ok
    }
  }
}

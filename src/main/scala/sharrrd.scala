package com.todesking.sharrrd

import scala.collection.immutable.{SortedMap, SortedSet, TreeMap}

class Sharding[KeyT, HashT, RealNodeT](
  hasher:KeyT => HashT,
  val currentHashRing:HashRing[HashT, RealNodeT],
  // sequence of old rings(newer first)
  val oldHashRings:Seq[HashRing[HashT, RealNodeT]]
) {
  def hashOf(key:KeyT):HashT = hasher(key)

  def realNodeOf(key:KeyT):RealNodeT = currentHashRing.realNodeOf(hashOf(key))

  def operate[A](key:KeyT)(f:(RealNodeT) => A):A =
    operateUntil(key, 0) {(_, node) => Some(f(node))}.get

  def operateUntil[A](key:KeyT, maxHistoryDepth:Int)(f:(HashRing[HashT, RealNodeT], RealNodeT) => Option[A]):Option[A] = {
    val hash = hashOf(key)
    f(currentHashRing, currentHashRing.realNodeOf(hash)) orElse operateOldUntil(hash, maxHistoryDepth)(f)
  }

  def operateOldUntil[A](hash:HashT, maxHistoryDepth:Int)(f:(HashRing[HashT, RealNodeT], RealNodeT) => Option[A]):Option[A] = {
    oldHashRings.take(maxHistoryDepth).foreach {ring =>
      f(ring, ring.realNodeOf(hash)) match {
        case s@Some(_) => return s
        case None =>
      }
    }
    None
  }
}

trait HashRing[HashT, RealNodeT] {
  def realNodeOf(hash:HashT):RealNodeT
}

trait FlexibleHashRing[HashT, RealNodeT, SelfT <: HashRing[HashT, RealNodeT]] extends HashRing[HashT, RealNodeT] {
  def add(nodes:RealNodeT*):SelfT
  def remove(nodes:RealNodeT*):SelfT
}

trait AssignmentPolicy[HashT, RealNodeT] {
  trait Assigner {
    def assign(assigned:Set[HashT], node:RealNodeT):Seq[HashT]
    def snapshot():AssignmentPolicy[HashT, RealNodeT]
  }
  def newAssigner():Assigner
}

object SerializableUtil {
  def clone[A <: java.io.Serializable](value:A):A = {
    val pIn = new java.io.PipedInputStream()
    val pOut = new java.io.PipedOutputStream(pIn)

    val oOut = new java.io.ObjectOutputStream(pOut)
    val oIn = new java.io.ObjectInputStream(pIn)

    try {
      oOut.writeObject(value)
      oIn.readObject().asInstanceOf[A]
    } finally {
      try { oOut.close() } finally { oIn.close() }
    }
  }
}

trait RandomSource[A] {
  def nextValue():A
  def copy():RandomSource[A]
}

object RandomSource {
  def fromJavaRandomInt(r:java.util.Random):RandomSource[Int] = new RandomSource[Int] {
    override def nextValue():Int = r.nextInt()
    override def copy():RandomSource[Int] = fromJavaRandomInt(SerializableUtil.clone(r))
  }
}

class DefaultAssignmentPolicy[HashT, RealNodeT](assignPerNode:Int, rand:RandomSource[HashT]) extends AssignmentPolicy[HashT, RealNodeT] {
  val randomProto:RandomSource[HashT] = rand.copy()

  def newAssigner() = new Assigner {
    val random = randomProto.copy()
    def assign(assigned:Set[HashT], node:RealNodeT):Seq[HashT] = {
      (0 until assignPerNode) map { _ =>
        var r = random.nextValue()
        while(assigned.contains(r)) r = random.nextValue()
        r
      }
    }
    def snapshot() = new DefaultAssignmentPolicy(assignPerNode, random)
  }
}

class DefaultHashRing[HashT, RealNodeT](
  val table:SortedMap[HashT, RealNodeT],
  val assignmentPolicy:AssignmentPolicy[HashT, RealNodeT]
) extends FlexibleHashRing[HashT, RealNodeT, DefaultHashRing[HashT, RealNodeT]] {
  def this(assignmentPolicy:AssignmentPolicy[HashT, RealNodeT])(implicit ev:Ordering[HashT]) =
    this(SortedMap.empty[HashT, RealNodeT], assignmentPolicy)

  override def realNodeOf(hash:HashT):RealNodeT = {
    table.from(hash).values.headOption getOrElse table.values.head
  }

  override def add(nodes:RealNodeT*):DefaultHashRing[HashT, RealNodeT] = {
    val policy = assignmentPolicy.newAssigner()
    val assignments = for {
        node <- nodes
        key <- policy.assign(table.keySet, node)
      } yield key -> node

    new DefaultHashRing(table ++ assignments, policy.snapshot())
  }

  override def remove(nodes:RealNodeT*):DefaultHashRing[HashT, RealNodeT] = {
    new DefaultHashRing(nodes.foldLeft(table) {(a, x) => a -- a.filter{case (k, v) => v == x}.keys}, assignmentPolicy)
  }
}

abstract class Migration[K, H, N, V, S <: Sharding[K, H, N]] {
  val sharding:S
  def migrate(oldNode:N, key:K):Unit = {
    val newNode = sharding.realNodeOf(key)
    if(oldNode == newNode) return
    if(exists(newNode, key)) throw new AssertionError
    get(oldNode, key) map {value =>
      put(newNode, key, value)
      delete(oldNode, key)
    }
  }

  def get(node:N, key:K):Option[V]
  def put(node:N, key:K, value:V):Unit
  def delete(node:N, key:K):Unit
  def exists(node:N, key:K):Boolean = get(node, key).isEmpty.unary_!
}

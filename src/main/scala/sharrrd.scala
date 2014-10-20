package com.todesking.sharrrd

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
    operate0(currentHashRing, hash, f) orElse operateOldUntil(hash, maxHistoryDepth)(f)
  }

  def operateOldUntil[A](hash:HashT, maxHistoryDepth:Int)(f:(HashRing[HashT, RealNodeT], RealNodeT) => Option[A]):Option[A] = {
    oldHashRings.take(maxHistoryDepth).foreach {ring =>
      operate0(ring, hash, f) match {
        case s@Some(_) => return s
        case None =>
      }
    }
    None
  }

  private def operate0[A](hashRing:HashRing[HashT, RealNodeT], hash:HashT, f:(HashRing[HashT, RealNodeT], RealNodeT) => A):A =
    f(hashRing, hashRing.realNodeOf(hash))
}

trait HashRing[HashT, RealNodeT] {
  def realNodeOf(hash:HashT):RealNodeT
}

trait FlexibleHashRing[HashT, RealNodeT] extends HashRing[HashT, RealNodeT] {
  def add(nodes:Seq[RealNodeT]):this.type
  def remove(nodes:Seq[RealNodeT]):this.type
}

object HashRing {
  import scala.collection.immutable.{SortedMap, SortedSet, TreeMap}

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

  class CloneableRandom(random:java.util.Random) extends java.lang.Cloneable {
    def this(seed:Long) = this(new java.util.Random(seed))
    def nextInt():Int = random.nextInt()

    override def clone():CloneableRandom = new CloneableRandom(SerializableUtil.clone(random))
  }

  class DefaultAssignmentPolicy[RealNodeT](assignPerNode:Int, r:CloneableRandom) extends AssignmentPolicy[Int, RealNodeT] {
    def this(assignPerNode:Int, seed:Long) = this(assignPerNode, new CloneableRandom(new java.util.Random(seed)))
    val randomProto:CloneableRandom = r.clone()

    def newAssigner() = new Assigner {
      val random = randomProto.clone()
      def assign(assigned:Set[Int], node:RealNodeT):Seq[Int] = {
        (0 until assignPerNode) map { _ =>
          var r = random.nextInt()
          while(assigned.contains(r)) r = random.nextInt()
          r
        }
      }
      def snapshot() = new DefaultAssignmentPolicy(assignPerNode, random)
    }
  }

  abstract class DefaultImpl[HashT, RealNodeT](
    val table:SortedMap[HashT, RealNodeT],
    val assignmentPolicy:AssignmentPolicy[HashT, RealNodeT]
  ) extends FlexibleHashRing[HashT, RealNodeT] {
    override def realNodeOf(hash:HashT):RealNodeT = {
      table.from(hash).values.headOption getOrElse table.values.head
    }
    override def add(nodes:Seq[RealNodeT]):this.type = {
      val policy = assignmentPolicy.newAssigner()
      val assignments = for {
          node <- nodes
          key <- policy.assign(table.keySet, node)
        } yield key -> node

      newInstance(table ++ assignments, policy.snapshot())
    }
    override def remove(nodes:Seq[RealNodeT]):this.type = {
      newInstance(nodes.foldLeft(table) {(a, x) => a -- a.filter{case (k, v) => v == x}.keys}, assignmentPolicy)
    }

    protected def newInstance(table:SortedMap[HashT, RealNodeT], policy:AssignmentPolicy[HashT, RealNodeT]):this.type
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

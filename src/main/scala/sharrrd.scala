package com.todesking.sharrrd

trait Sharding[KeyT, HashT, RealNodeT] {
  type NodeMapT = NodeMap[RealNodeT]

  val currentNodeMap:NodeMapT
  val nodeMapHistory:Seq[NodeMapT]
  val hashRing:HashRing[KeyT, HashT]

  def realNodeOf(key:KeyT) = currentNodeMap.realNodeOf(hashRing.virtualNodeFromKey(key))

  def operate[A](key:KeyT)(f:(RealNodeT) => A):A =
    operateUntil(key, 0) {(_, node) => Some(f(node))}.get

  def operateUntil[A](key:KeyT, maxHistoryDepth:Int)(f:(NodeMapT, RealNodeT) => Option[A]):Option[A] = {
    val vnode = hashRing.virtualNodeFromKey(key)
    operate0(currentNodeMap, vnode, f) orElse operateHistoryUntil(vnode, maxHistoryDepth)(f)
  }

  def operateHistoryUntil[A](vnode:VirtualNode, maxHistoryDepth:Int)(f:(NodeMapT, RealNodeT) => Option[A]):Option[A] = {
    nodeMapHistory.take(maxHistoryDepth).foreach {nm =>
      operate0(nm, vnode, f) match {
        case s@Some(_) => return s
        case None =>
      }
    }
    None
  }

  private def operate0[A](nodeMap:NodeMapT, vnode:VirtualNode, f:(NodeMapT, RealNodeT) => A):A =
    f(nodeMap, nodeMap.realNodeOf(vnode))
}

trait HashRing[KeyT, HashT] {
  def hashOf(key:KeyT):HashT
  def virtualNodeFromKey(key:KeyT):VirtualNode = virtualNodeFromHash(hashOf(key))
  def virtualNodeFromHash(h:HashT):VirtualNode
}

object HashRing {
  abstract class DefaultImpl[KeyT, HashT] extends HashRing[KeyT, HashT] {
    val table:collection.immutable.SortedMap[HashT, VirtualNode]
    override def virtualNodeFromHash(h:HashT):VirtualNode = {
      table.from(h).values.headOption getOrElse table.values.head
    }
  }
}

case class VirtualNode(num:Int) extends Ordered[VirtualNode] {
  override def compare(that:VirtualNode):Int = implicitly[Ordering[Int]].compare(this.num, that.num)
}

trait NodeMap[RealNodeT] {
  def realNodeOf(vn:VirtualNode):RealNodeT
}

object NodeMap {
  class DefaultImpl[RealNodeT](mapping:Map[VirtualNode, RealNodeT]) extends NodeMap[RealNodeT] {
    override def realNodeOf(vn:VirtualNode) = mapping(vn)
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

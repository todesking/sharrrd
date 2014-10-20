# Sharrrd: Basic types and logics for Consistent Hashing with Scala

## Basic architecture

```
Key =(hash algorithm)=> Hash code =(hash to node mapping)=> Real node
                                  ~ HashRing[KeyT, HashT] ~
    ~~~~~~~~~~~~~~~~~~~~ Sharding[KeyT, HashT, NodeT] ~~~~~
```

You can customize hash and mapping algorithm. Some default implementations available in `HashRing.DefaultImpl` and
`NodeMap.DefaultImpl`.

## Interface

```scala
trait Sharding[KeyT, HashT, RealNodeT] {
  type NodeMapT = NodeMap[RealNodeT]

  def realNodeOf(key:KeyT)
  def operate[A](key:KeyT)(f:(RealNodeT) => A):A
  def operateUntil[A](key:KeyT, maxHistoryDepth:Int)(f:(NodeMapT, RealNodeT) => Option[A]):Option[A]
}
```

# Sharrrd: Consistent Hashing for Scala

## Basic architecture

```
Key =(hash algorithm)=> Hash code ===(hash to node mapping)==> Real node
                                  ~HashRing[HashT, RealNodeT]~
                                  ~~~~(                  )~~~~
                                  ~~~~(previous hash ring)~~~~
                                  ~~~~(                  )~~~~
    ~~~~~~~~~~~~~~~~ Sharding[KeyT, HashT, RealNodeT] ~~~~~~~~
```

You can customize hash and mapping algorithm. Default implementation available at `DefaultHashRing`.

## Simple example for default implementation

```scala
import com.todesking.sharrrd

val redisInstances = getRedisInstances()
val randomSeed = 0L

val assignmentPolicy = new DefaultAssignmentPolicy(
  assignPerNode = 100,
  RandomSource.fromJavaRandomInt(new java.util.Random(randomSeed))
)

// Sharding that key = String, hash = Int, node = RedisInstance
val sharding = new Sharding[String, Int, RedisInstance](
  key => myHashFunction(key),
  new DefaultHashRing(assignmentpolicy).add(redisInstances:_*),
  Seq() // recent hash ring definitions(newer first)
)

def put(key:String, value:Int):Unit = {
  val redis = sharding.realNodeOf(key)
  redis.put(key, value)

  // or
  sharding.operate(key) { redis => redis.put(key, value) }
}

// Read data from redis. Dig down recent 10 hash ring histories.
def get(key:String):Option[Int] = {
  sharding.operateUntil(key, 10) { (hashRing, redis) => redis.getOption(key) }
}
```


## Interface

```scala
class Sharding[KeyT, HashT, RealNodeT] {
  val currentHashRing:HashRing[HashT, RealNodeT]
  val oldHashRings:Seq[HashRing[HashT, RealNodeT]] // sequence of old rings(newer first)

  def realNodeOf(key:KeyT):RealNodeT
  def operate[A](key:KeyT)(f:(RealNodeT) => A):A
  def operateUntil[A](key:KeyT, maxHistoryDepth:Int)(f:(HashRing[HashT, RealNodeT], RealNodeT) => Option[A]):Option[A]
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

class DefaultAssignmentPolicy[HashT, RealNodeT](assignPerNode:Int, rand:RandomSource[HashT]) extends AssignmentPolicy[HashT, RealNodeT]

class DefaultHashRing[HashT, RealNodeT](
  val table:SortedMap[HashT, RealNodeT],
  val assignmentPolicy:AssignmentPolicy[HashT, RealNodeT]
) extends FlexibleHashRing[HashT, RealNodeT, DefaultHashRing[HashT, RealNodeT]] {
  def this(assignmentPolicy:AssignmentPolicy[HashT, RealNodeT])(implicit ev:Ordering[HashT])
}
```

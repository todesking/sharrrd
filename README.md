# Sharrrd: Basic types and logics for Consistent Hashing with Scala

## Basic architecture

```
Key =(hash algorithm)=> Hash code =(hash to node mapping)=> Real node
                                  ~ HashRing[KeyT, HashT] ~
    ~~~~~~~~~~~~~~~~~~~~ Sharding[KeyT, HashT, NodeT] ~~~~~
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

val sharding = new Sharding[String, Int, RedisInstance](
  key => myHashFunction(key),
  new DefaultHashRing(assignmentpolicy).add(redisInstances:_*),
  Seq()
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
trait Sharding[KeyT, HashT, RealNodeT] {
  type NodeMapT = NodeMap[RealNodeT]

  def realNodeOf(key:KeyT)
  def operate[A](key:KeyT)(f:(RealNodeT) => A):A
  def operateUntil[A](key:KeyT, maxHistoryDepth:Int)(f:(NodeMapT, RealNodeT) => Option[A]):Option[A]
}
```

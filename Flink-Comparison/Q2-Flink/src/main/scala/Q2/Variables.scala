package Q2.utils
import scala.collection.mutable
import org.apache.flink.util.MathUtils

// Point class
class Point(var id: Int,
            var tag: Int,
            var sid: Int,
            var x: Int, // sensor coordinates
            var y: Int, // sensor coordinates
            var z: Int, // sensor coordinates
            var v: Int, // velocity
            var a: Int, // acceleration
            var vx: Int, // direction x
            var vy: Int, // direction y
            var vz: Int, // direction z
            var ax: Int, // acceleration x
            var ay: Int, // acceleration y
            var az: Int, // acceleration z
            var timestamp: java.sql.Timestamp)
{
    // constructor
    def this() = this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new java.sql.Timestamp(0L))
}

// WinResult class
class WinResult(var id: Long,
                var centroid1: Point,
                var centroid2: Point,
                var centroid3: Point,
                var timestamp: java.sql.Timestamp)
{
    // constructor
    def this() = this(0, new Point(), new Point(), new Point(), new java.sql.Timestamp(0L))
}

// KeyGenerator class
class KeyGenerator(val partitions: Int, val maxPartitions: Int)
{
    // constructor
    def this(partitions: Int) = this(partitions, 128)

    val ids = Stream.from(1).iterator
    val cache = mutable.HashMap[Int, mutable.Queue[Int]]()

    // next method
    def next(targetPartition: Int): Int = {
        val queue = cache.getOrElseUpdate(targetPartition, mutable.Queue[Int]())
        if (queue.size == 0) {
            var found = false
            while (!found) {
                val id = ids.next
                val partition = (MathUtils.murmurHash(id) % maxPartitions) * partitions / maxPartitions
                cache.getOrElseUpdate(partition, mutable.Queue[Int]()).enqueue(id)
                if (partition == targetPartition) {
                    found = true
                }
            }
        }
        queue.dequeue()
    }
}

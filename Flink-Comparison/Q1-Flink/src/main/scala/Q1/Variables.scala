package Q1.utils
import scala.collection.mutable
import org.apache.flink.util.MathUtils

// Event class
class Event(var id: Long,
            var tag: Int,
            var bid_price: Float,
            var bid_size: Int,
            var ask_price: Float,
            var ask_size: Int,
            var event_time: java.sql.Timestamp)
{
    // constructor
    def this() = this(0, 0, 0, 0, 0, 0, new java.sql.Timestamp(0L))
}

// WinResult class
class WinResult(var id: Long,
                var p0_bid: Float,
                var p1_bid: Float,
                var p2_bid: Float,
                var p0_ask: Float,
                var p1_ask: Float,
                var p2_ask: Float,
                var open_bid: Float,
                var close_bid: Float,
                var high_bid: Float,
                var low_bid: Float,
                var open_ask: Float,
                var close_ask: Float,
                var high_ask: Float,
                var low_ask: Float,
                var event_time: java.sql.Timestamp)
{
    // constructor
    def this() = this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new java.sql.Timestamp(0L))
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

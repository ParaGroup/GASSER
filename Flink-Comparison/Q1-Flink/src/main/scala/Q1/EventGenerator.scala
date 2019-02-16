package Q1

import Q1.utils._
import scala.util.Random
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

// EventGenerator class
class EventGenerator(num_tuples: Long, rate: Int) extends RichParallelSourceFunction[Event]
{
    // variables
    var running = true

    // run method
    override def run(sourceContext: SourceContext[Event]): Unit = {
        var sent = 0
        var count = 0
        val random_generator = scala.util.Random
        var lastTime = System.currentTimeMillis()
        // generation loop
        while ((sent < num_tuples) && running) {
            var id = sent
            var tag = 0
            var bid_price = (random_generator.nextFloat() * 100) + 100
            var bid_size = random_generator.nextInt(200)
            var ask_price = (random_generator.nextFloat() * 100) + 100
            var ask_size = random_generator.nextInt(200)
            var ts = System.currentTimeMillis()
            val event = new Event(
                id,
                tag,
                bid_price,
                bid_size,
                ask_price,
                ask_size,
                new java.sql.Timestamp(ts))
            sourceContext.collect(event)
            sent = sent + 1
            count = count + 1
            // check the rate
            if (count == rate) {
                count = 0
                var elapsedTime = (1000 - (System.currentTimeMillis() - lastTime))
                if (elapsedTime > 0)
                    Thread.sleep(elapsedTime)
                lastTime = System.currentTimeMillis()
            }
        }
        sourceContext.close()
        running = false
        println("[Source] Generation done with " + sent + " generated events")
    }

    // cancel method
    override def cancel(): Unit = {
        running = false
    }
}

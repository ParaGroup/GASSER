package Q1

import Q1.utils._
import scala.math._
import scala.io.Source
import java.sql.Timestamp
import scala.util.MurmurHash
import scala.collection.mutable.Set
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.runtime.state.KeyGroupRangeAssignment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

object Query1 {

    // TimestampingSink operator
    class TimestampingSink extends RichSinkFunction[WinResult]
    {
        // variables
        private var latency = 0L
        private var avgLatency = 0L
        private var curTime = -1L
        private var counter = 0

        // invoke method
        override def invoke(result: WinResult): Unit = {
            curTime = System.currentTimeMillis()
            latency = (curTime - result.event_time.getTime)
            counter += 1
            avgLatency += latency
        }

        // close method
        override def close(): Unit = {
            println("[Sink] Received " + counter + " results, average latency " + (avgLatency.toFloat)/(counter.toFloat) + " ms")
        }
    }

    // ThroughputLogger operator
    class ThroughputLogger() extends FlatMapFunction[Event, Event]
    {
        // variables
        private var lastTotalReceived: Long = 0L
        private var lastTime: Long = System.currentTimeMillis()
        private var totalReceived: Long = 0L
        private var averageThroughput = 0d
        private var throughputCounter = 0
        private var throughputSum = 0d
  
        // flatmap method
        override def flatMap(event: Event, collector: Collector[Event]): Unit = {
            totalReceived += 1
            var currentTime = System.currentTimeMillis()
            // log every second
            if (currentTime - lastTime > 1000) {
                val throughput = (totalReceived - lastTotalReceived) / (currentTime - lastTime) * 1000.0d
                if (throughput != 0) {
                    throughputCounter += 1
                    throughputSum += throughput
                    averageThroughput = throughputSum / throughputCounter
                }
                println("[Throughput Monitor] Instantaneous throughput " + throughput + "  tuples/second, average throughput " + averageThroughput + " tuples/second")
                lastTime = currentTime
                lastTotalReceived = totalReceived          
            }
            collector.collect(event)
        }
    }

    // WinTagger operator
    class WinTagger(W: Int, S: Int, Nw: Int, key_list: Array[Int], version: String) extends FlatMapFunction[Event, Event]
    {
        // flatmap method
        override def flatMap(event: Event, collector: Collector[Event]): Unit = {
            // WF emulation version
            if (version.equals("WF")) {
                // determine the first and the last window containing the input event
                var first_w = 0
                var last_w = 0
                if (event.id + 1 < W)
                    first_w = 0
                else
                    first_w = ceil(((event.id).toFloat + 1f - W.toFloat) / S.toFloat).toInt
                last_w = ceil(((event.id).toFloat + 1)/ S.toFloat).toInt - 1;
                // determine the workers that must receive a copy of the input event
                var i = first_w
                var cnt = 0
                var set_id = Set[Int]()
                while ((i <= last_w) && (cnt < Nw)) {
                    set_id.add(i % Nw)
                    cnt += 1
                    i += 1
                }
                // send a copy of the event with the proper tag
                for (idx <- set_id) {
                    var tag = key_list(idx)
                    var tagged_event = new Event(event.id,
                                                 tag,
                                                 event.bid_price,
                                                 event.bid_size,
                                                 event.ask_price,
                                                 event.ask_size,
                                                 event.event_time)
                    collector.collect(tagged_event)
                }
            }
            // otherwise, KF emulation version
            else {
                var tag = key_list((event.id % Nw).toInt)
                var tagged_event = new Event(event.id,
                                             tag,
                                             event.bid_price,
                                             event.bid_size,
                                             event.ask_price,
                                             event.ask_size,
                                             event.event_time)
                collector.collect(tagged_event)
            }
        }
    }

    // Query1WindowFunction class
    class Query1WindowFunction extends ProcessWindowFunction[Event, WinResult, Tuple, GlobalWindow]
    {
        // process method
        def process(tag: Tuple, context: Context, input: Iterable[Event], out: Collector[WinResult]): Unit = {
            var sum = 0f
            var first_ts = (input.head).event_time
            var sum_x_bid = 0f
            var sum_y_bid = 0f
            var sum_x_square_bid = 0f
            var sum_x_cube_bid = 0f
            var sum_x_quad_bid = 0f
            var sum_x_times_y_bid = 0f
            var sum_x_sq_times_y_bid = 0f
            var sum_x_ask = 0f
            var sum_y_ask = 0f
            var sum_x_square_ask = 0f
            var sum_x_cube_ask = 0f
            var sum_x_quad_ask = 0f
            var sum_x_times_y_ask = 0f
            var sum_x_sq_times_y_ask = 0f
            var numb_points_bid = 0
            var numb_points_ask = 0
            // first loop over the window events
            for (in <- input) {
                var x = (in.event_time).getNanos() - first_ts.getNanos()
                // bid part
                if (in.bid_size > 0) {
                    var y_bid = in.bid_price
                    sum_x_bid += x
                    sum_y_bid += y_bid
                    var xsq = x*x
                    sum_x_square_bid += xsq
                    sum_x_cube_bid += xsq*x
                    sum_x_quad_bid += xsq*xsq
                    sum_x_times_y_bid += x*y_bid
                    sum_x_sq_times_y_bid += xsq*y_bid
                    numb_points_bid += 1
                }
                // ask part
                if (in.ask_size > 0) {
                    var y_ask = in.ask_price
                    sum_x_ask += x
                    sum_y_ask += y_ask
                    var xsq = x*x
                    sum_x_square_ask += xsq
                    sum_x_cube_ask += xsq*x
                    sum_x_quad_ask += xsq*xsq
                    sum_x_times_y_ask += x*y_ask
                    sum_x_sq_times_y_ask += xsq*y_ask
                    numb_points_ask += 1
                }
            }
            // compute bid coefficient
            var a_bid = ((sum_x_sq_times_y_bid*sum_x_square_bid)-(sum_x_times_y_bid*sum_x_cube_bid))
            a_bid /= ((sum_x_square_bid*sum_x_quad_bid)-sum_x_cube_bid*sum_x_cube_bid)
            var b_bid = ((sum_x_times_y_bid*sum_x_quad_bid)-(sum_x_sq_times_y_bid*sum_x_cube_bid))
            b_bid /= ((sum_x_square_bid*sum_x_quad_bid)-(sum_x_cube_bid*sum_x_cube_bid))
            var c_bid = sum_y_bid/numb_points_bid - b_bid*sum_x_bid/numb_points_bid - a_bid*sum_x_square_bid/numb_points_bid
            var p0_bid = c_bid
            var p1_bid = b_bid
            var p2_bid = a_bid
            // compute the ask coefficient
            var a_ask = ((sum_x_sq_times_y_ask*sum_x_square_ask)-(sum_x_times_y_ask*sum_x_cube_ask))
            a_ask /= ((sum_x_square_ask*sum_x_quad_ask)-sum_x_cube_ask*sum_x_cube_ask)
            var b_ask = ((sum_x_times_y_ask*sum_x_quad_ask)-(sum_x_sq_times_y_ask*sum_x_cube_ask))
            b_ask /= ((sum_x_square_ask*sum_x_quad_ask)-(sum_x_cube_ask*sum_x_cube_ask))
            var c_ask = sum_y_ask/numb_points_ask - b_ask*sum_x_ask/numb_points_ask - a_ask*sum_x_square_ask/numb_points_ask
            var p0_ask = c_ask
            var p1_ask = b_ask
            var p2_ask = a_ask
            var high_ask = sum
            var id = 1 // 1 sarebbe la window length
            var low_bid = 10000f
            var low_ask = 10000f
            var high_bid = 0f
            high_ask = 0f
            var open_bid = 0f
            var open_ask = 0f
            var last_valid_bid = 0f
            var last_valid_ask = 0f
            // second loop over the window events
            for (in <- input) {
                if (in.bid_size > 0) {
                    if (open_bid == 0)
                        open_bid = in.bid_price
                    if (in.bid_price > high_bid)
                        high_bid = in.bid_price
                    if (in.bid_price < low_bid)
                        low_bid = in.bid_price
                }
                if (in.ask_size > 0) {
                    if (open_ask == 0)
                        open_ask = in.ask_price
                    if (in.ask_price > high_ask)
                        high_ask = in.ask_price
                    if (in.ask_price < low_ask)
                        low_ask = in.ask_price
                }
            }
            var close_bid = (input.last).bid_price
            var close_ask = (input.last).ask_price
            // the timestamp of the window result is the one of the last tuple
            var event_time = (input.last).event_time
            // prepare the result of the window
            var result = new WinResult(id,
                                       p0_bid,
                                       p1_bid,
                                       p2_bid,
                                       p0_ask,
                                       p1_ask,
                                       p2_ask,
                                       open_bid,
                                       close_bid,
                                       high_bid,
                                       low_bid,
                                       open_ask,
                                       close_ask,
                                       high_ask,
                                       low_ask,
                                       event_time)
            out.collect(result)
        }
    }

    // main
    def main(args: Array[String]): Unit = {
        // configuration parameters from command line
        val params: ParameterTool = ParameterTool.fromArgs(args)
        var stream_length: Int = params.getInt("streamLength", 100000)
        var win_length: Int = params.getInt("winLength", 1000)
        var slide_length: Int = params.getInt("slideLength", 10)
        var parallelism: Int = params.getInt("parallelism", 1)
        var version: String = params.get("version", "KF")
        var rate: Int = params.getInt("rate", -1)
        require(parallelism > 0, "Parallelism needs to be a positive integer")
        if (version.equals("WF"))
            require(parallelism*slide_length <= win_length, "Sliding factor too small for this parallelism")
        println("[Configuration]: streamLength " +  stream_length + ", winLength " +  win_length + ", slideLength " +  slide_length + ", parallelism " + parallelism + ", version " + version + ", rate " + rate + " tuples/sec")

        // create the keys used by the WinTagger operator
        val keyGenerator = new KeyGenerator(parallelism, KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism))
        var key_list = new Array[Int](parallelism)
        for (i <- 0 to parallelism-1) {
            var key_value = keyGenerator.next(i)
            key_list(i) = key_value
        }

        // create the environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.getConfig.enableObjectReuse()

        // add source operator
        val events = env.addSource(new EventGenerator(stream_length, rate)).setParallelism(1)

        // add WinTagger operator
        val taggedStream = events
            .flatMap(new WinTagger(win_length, slide_length, parallelism, key_list, version)).setParallelism(parallelism)

        // add sliding-window operator
        val used_slide = if (version.equals("KF")) slide_length else slide_length*parallelism
        val windowedStream = taggedStream
            .keyBy("tag")
            .countWindow(win_length, used_slide)
            .process(new Query1WindowFunction()).setParallelism(parallelism)

        // add sink operator
        windowedStream.addSink(new TimestampingSink()).setParallelism(parallelism)

        // execute the application
        env.execute("Query 1 Benchmark")
    }
}

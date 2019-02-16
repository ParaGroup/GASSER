package Q2

import Q2.utils._
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

object Query2 {

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
            latency = (curTime - result.timestamp.getTime)
            counter += 1
            avgLatency += latency
        }

        // close method
        override def close(): Unit = {
            println("[Sink] Received " + counter + " results, average latency " + (avgLatency.toFloat)/(counter.toFloat) + " ms")
        }
    }

    // ThroughputLogger operator
    class ThroughputLogger() extends FlatMapFunction[Point, Point]
    {
        // variables
        private var lastTotalReceived: Long = 0L
        private var lastTime: Long = System.currentTimeMillis()
        private var totalReceived: Long = 0L
        private var averageThroughput = 0d
        private var throughputCounter = 0
        private var throughputSum = 0d
  
        // flatmap method
        override def flatMap(point: Point, collector: Collector[Point]): Unit = {
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
            collector.collect(point)
        }
    }

    // WinTagger operator
    class WinTagger(W: Int, S: Int, Nw: Int, key_list: Array[Int], version: String) extends FlatMapFunction[Point, Point]
    {
        // flatmap method
        override def flatMap(point: Point, collector: Collector[Point]): Unit = {
            // WF emulation version
            if (version.equals("WF")) {
                // determine the first and the last window containing the input point
                var first_w = 0
                var last_w = 0
                if (point.id + 1 < W)
                    first_w = 0
                else
                    first_w = ceil(((point.id).toFloat + 1f - W.toFloat) / S.toFloat).toInt
                last_w = ceil(((point.id).toFloat + 1)/ S.toFloat).toInt - 1;
                // determine the workers that must receive a copy of the input point
                var i = first_w
                var cnt = 0
                var set_id = Set[Int]()
                while ((i <= last_w) && (cnt < Nw)) {
                    set_id.add(i % Nw)
                    cnt += 1
                    i += 1
                }
                // send a copy of the point with the proper tag
                for (idx <- set_id) {
                    var tag = key_list(idx)
                    var tagged_point = new Point(point.id,
                                                 tag,
                                                 point.sid,
                                                 point.x,
                                                 point.y,
                                                 point.z,
                                                 point.v,
                                                 point.a,
                                                 point.vx,
                                                 point.vy,
                                                 point.vz,
                                                 point.ax,
                                                 point.ay,
                                                 point.az,
                                                 point.timestamp)
                    collector.collect(tagged_point)
                }
            }
            // otherwise, KF emulation version
            else {
                var tag = key_list((point.id % Nw).toInt)
                    var tagged_point = new Point(point.id,
                                                 tag,
                                                 point.sid,
                                                 point.x,
                                                 point.y,
                                                 point.z,
                                                 point.v,
                                                 point.a,
                                                 point.vx,
                                                 point.vy,
                                                 point.vz,
                                                 point.ax,
                                                 point.ay,
                                                 point.az,
                                                 point.timestamp)
                    collector.collect(tagged_point)
            }
        }
    }

    // Query2WindowFunction class
    class Query2WindowFunction extends ProcessWindowFunction[Point, WinResult, Tuple, GlobalWindow]
    {
    	// calculateDistance method
    	def calculateDistance(p1: Point, p2: Point): Double = {
			var dist = 0f
			var tmp = p1.x - p2.x
			dist += tmp * tmp
			tmp = p1.y - p2.y
			dist += tmp * tmp
			return sqrt(dist) 		
    	}

        // AssignAndComputeNewCluster method
        def AssignAndComputeNewCluster(data: Seq[Point], centroids: Array[Point], n_centroids: Int): Unit = {
        	var count = new Array[Int](3)
        	var tmpx = new Array[Double](3)
        	var tmpy = new Array[Double](3)
        	for (i <- 0 to 2) {
        		count(i) = 0
        		tmpx(i) = 0f
        		tmpy(i) = 0f
        	}
        	// scan all the points in the window
        	for (p <- data) {
        		// assign the point to a cluster
        		var assigned_cluster = 0
        		var min_dist = calculateDistance(p, centroids(0))
        		for (j <- 1 to n_centroids-1) {
        			var dist = calculateDistance(p, centroids(j))
        			if (dist < min_dist) {
						min_dist = dist
						assigned_cluster = j
					}
        		}
				// add to the proper centroid
				count(assigned_cluster) += 1
				tmpx(assigned_cluster) += p.x
				tmpy(assigned_cluster) += p.y
        	}
        	for (i <- 0 to n_centroids-1) {
				centroids(i).x = (tmpx(i) / count(i)).toInt
				centroids(i).y = (tmpy(i) / count(i)).toInt
			}
        }

        // process method
        def process(tag: Tuple, context: Context, input: Iterable[Point], out: Collector[WinResult]): Unit = {
        	var id = 0
        	val n_centroids = 3
	    	val data = input.toSeq
	    	var centroids = new Array[Point](n_centroids)
	    	for (i <- 0 to n_centroids-1) {
	    		centroids(i) = data(i)
	    	}
 			var iteration = 0
 			// start iterations of Kmeans
 			while (iteration < 10) { // max 10 iterations are possible
 				AssignAndComputeNewCluster(data, centroids, n_centroids)
 				iteration += 1
 			}
 			var result = new WinResult(id,
 									   centroids(0),
 									   centroids(1),
 									   centroids(2),
 									   data(data.length-1).timestamp)
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
        val filename = "/home/mencagli/Q2-Flink/200K_bin"

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
        val events = env.addSource(new EventGenerator(stream_length, rate, filename)).setParallelism(1)

        // add WinTagger operator
        val taggedStream = events
            .flatMap(new WinTagger(win_length, slide_length, parallelism, key_list, version)).setParallelism(parallelism)

        // add sliding-window operator
        val used_slide = if (version.equals("KF")) slide_length else slide_length*parallelism
        val windowedStream = taggedStream
            .keyBy("tag")
            .countWindow(win_length, used_slide)
            .process(new Query2WindowFunction()).setParallelism(parallelism)

        // add sink operator
        windowedStream.addSink(new TimestampingSink()).setParallelism(parallelism)

        // execute the application
        env.execute("Query 2 Benchmark")
    }
}

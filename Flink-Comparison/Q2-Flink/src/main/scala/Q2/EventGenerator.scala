package Q2

import Q2.utils._
import scala.util.Random
import java.nio.ByteBuffer
import scala.util.control._
import java.nio.file.{Files, Paths}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

// EventGenerator class
class EventGenerator(num_tuples: Long, rate: Int, filename: String) extends RichParallelSourceFunction[Point]
{
    // variables
    var running = true

    // run method
    override def run(sourceContext: SourceContext[Point]): Unit = {
        var sent = 0
        var count = 0
        var lastTime = System.currentTimeMillis()
        // read the input dataset file
        val byteArray = Files.readAllBytes(Paths.get(filename))
        val bb = java.nio.ByteBuffer.wrap(byteArray)
        bb.order(java.nio.ByteOrder.nativeOrder)
        println("[Source] Start reading tuples from file...")
        // generation loop
        while ((sent < num_tuples) && running) {
            val loop = new Breaks;
            loop.breakable {
                // read the dataset file and generate each tuple
                while (bb.hasRemaining() && running) {
                    val id = bb.getInt
                    val sid = bb.getInt
                    val ts = bb.getDouble
                    val x = bb.getInt
                    val y = bb.getInt
                    val z = bb.getInt
                    val v = bb.getInt
                    val a = bb.getInt
                    val vx = bb.getInt
                    val vy = bb.getInt
                    val vz = bb.getInt
                    val ax = bb.getInt
                    val ay = bb.getInt
                    val az = bb.getInt
                    val tag = 0
                    // generate the point
                    var point = new Point(id, tag, sid, x, y, z, v, a, vx, vy, vz, ax, ay, az, new java.sql.Timestamp(System.currentTimeMillis()))
                    sourceContext.collect(point)
                    sent += 1
                    count += 1
                    bb.position(bb.position + 4) // offset of 4 bytes
                    // check the rate
                    if (count == rate) {
                        count = 0
                        var elapsedTime = (1000 - (System.currentTimeMillis() - lastTime))
                        if (elapsedTime > 0)
                            Thread.sleep(elapsedTime)
                        lastTime = System.currentTimeMillis()
                    }
                    if (sent >= num_tuples)
                        loop.break
                }
                bb.rewind()
            }
        }
        sourceContext.close()
        running = false
        println("[Source] Generation done with " + sent + " generated points")
    }

    // cancel method
    override def cancel(): Unit = {
        running = false
    }
}

package com.seedotech;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 9000)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 9000 on Linux or nc -l -p 9000 on Windows
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class StreamingWordCount {
    final static int MAX_MEM_STATE_SIZE = 1000000000;
    final static String OUTPUT_FILE_PATH = "/home/raycad/tmp/raycad";
    // Computation window time in seconds
    final static int COMPUTATION_WINDOW_TIME = 1;

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'StreamingWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // Get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE, false));
//        env.setStateBackend(new FsStateBackend("file:///home/raycad/tmp/raycad/flink/checkpoints", false));

        // Get input data by connecting to the socket
        DataStream<String> source = env.socketTextStream(hostname, port, "\n");

        // Parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = source
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split(" ")) {
                            out.collect(new WordWithCount(word, 1));
                        }
                    }
                })

                .keyBy("word")
                .timeWindow(Time.seconds(COMPUTATION_WINDOW_TIME))

                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//                        long threadId = Thread.currentThread().getId();
//                        System.out.println(String.format(">>>> [RAYCAD] - Thread ID = %d - %s: %d", threadId, a.word, a.count + b.count));
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // Configure File sink
        StreamingFileSink<WordWithCount> sink = StreamingFileSink
                .forRowFormat(new Path(String.format(OUTPUT_FILE_PATH)),
                        (Encoder<WordWithCount>) (element, stream) -> {
                            PrintStream out = new PrintStream(stream);
                            out.println(element.toString());
                        })
                // Determine S3 folder for each element
                .withBucketAssigner(new WordWithCountBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        windowCounts.addSink(sink);

        // Print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Streaming Window WordCount");
    }

    /**
     * Output folder assigner
     */
    private static class WordWithCountBucketAssigner extends DateTimeBucketAssigner<WordWithCount> {

        @Override
        public String getBucketId(WordWithCount element, Context context) {
            String bucketId = super.getBucketId(element, context);
            System.out.println(String.format(">>>> [RAYCAD] - Bucket ID = %s. Data = %s", bucketId, element.toString()));
            return bucketId + "/" + element.word;
        }
    }
}
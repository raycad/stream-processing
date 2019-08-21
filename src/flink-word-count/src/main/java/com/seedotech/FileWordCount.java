package com.seedotech;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class FileWordCount {
    final static String DS_FILE_PATH = "file:///home/raycad/tmp/raycad/test1.txt";

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new FsStateBackend("file:///home/raycad/tmp/raycad/flink/checkpoints", false));

        // Get input data
        DataStream<String> source = env.readTextFile(DS_FILE_PATH);

        final int windowSize = 100;
        final int slideSize = 40;

        // Parse the data, group it and aggregate the counts
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
                .countWindow(windowSize, slideSize)
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//                        long threadId = Thread.currentThread().getId();
//                        System.out.println(String.format(">>>> [RAYCAD] - Thread ID = %d - %s: %d", threadId, a.word, a.count + b.count));
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // Print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("File WordCount");
    }
}
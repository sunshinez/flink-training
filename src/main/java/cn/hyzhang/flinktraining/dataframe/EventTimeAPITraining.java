package cn.hyzhang.flinktraining.dataframe;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class EventTimeAPITraining {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        WindowedStream<Transaction,Long, TimeWindow> windowedStream = env.addSource(new TransactionSource()).assignTimestampsAndWatermarks(new CustTimeExtractor()).keyBy(Transaction::getAccountId).timeWindow(Time.hours(3));

        windowedStream.reduce(new ReduceTransation()).addSink(new TrasactionSink()).name("transaction-sink");
        env.execute("event-app");
    }
}

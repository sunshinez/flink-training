package cn.hyzhang.flinktraining.dataframe;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class DataFrameAPITraining {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> source = env.addSource(new TransactionSource()).name("TransactionSource");
        DataStream<Alert> alert = source.keyBy(Transaction::getAccountId).process(new FraudDetector()).name("FraudDetector");
        alert.addSink(new AlertSink()).name("alertSink");
        env.execute("DataFrameAPITraining");
    }
}

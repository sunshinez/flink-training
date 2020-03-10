package cn.hyzhang.flinktraining.dataframe;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

public class FraudDetector extends KeyedProcessFunction<Long,Transaction, Alert> {
    final double  smallTrasactionThreshold = 1.0;
    final double  largeTrasactionThreshold = 500.0;
    final long timeThreshold = 10*1000;
    ValueState<Boolean> isUnsecure;
    ValueState<Boolean> isSmallTrasactionBefore;

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        System.out.println("------------------------onTimer called!------------------------");
        isUnsecure.clear();
        isSmallTrasactionBefore.clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("------------------------open called!------------------------");
        isSmallTrasactionBefore = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSmallTrasactionBefore", Types.BOOLEAN));
        isUnsecure = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isUnsecure", Types.BOOLEAN));
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        isSmallTrasactionBefore = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSmallTrasactionBefore", Types.BOOLEAN));
        isUnsecure = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isUnsecure", Types.BOOLEAN));
        if(isSmallTrasactionBefore.value()!=null && isUnsecure.value()!=null && isSmallTrasactionBefore.value() && isUnsecure.value()){
            if(transaction.getAmount()>largeTrasactionThreshold){
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
                isSmallTrasactionBefore.clear();
                isUnsecure.clear();
            }
            else {
                if(transaction.getAmount()<smallTrasactionThreshold){
                    isSmallTrasactionBefore.update(true);
                    isUnsecure.update(true);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeThreshold);
                }
            }
        }
        else {
            if(transaction.getAmount()<smallTrasactionThreshold){
                isSmallTrasactionBefore.update(true);
                isUnsecure.update(true);
                context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeThreshold);
            }
        }

    }
}

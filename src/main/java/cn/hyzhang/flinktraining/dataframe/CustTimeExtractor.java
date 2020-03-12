package cn.hyzhang.flinktraining.dataframe;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.walkthrough.common.entity.Transaction;

import javax.annotation.Nullable;

public class CustTimeExtractor implements AssignerWithPeriodicWatermarks<Transaction> {
    Watermark currentWatermark;
    Long[] list = {-5l,-4l,-3l,-2l,-1l,0l,1l,2l,3l,4l,5l};
    long currentTimestamp;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return currentWatermark;
    }

    @Override
    public long extractTimestamp(Transaction transaction, long l) {
        //int index = (int)Math.floor(Math.random()*11);
        long timestamp = transaction.getTimestamp();
        if(timestamp>currentTimestamp){
            currentTimestamp=timestamp;
            currentWatermark = new Watermark(timestamp);
        }
        //long timestamp = transaction.getTimestamp() + list[index] * 6 * 60 * 1000;
//        if(currentTimestamp != 0l && timestamp> currentTimestamp){
//            currentTimestamp=timestamp;
//        }
//        if(currentWatermark != null && currentWatermark.getTimestamp()>currentTimestamp){
//        }
//        else{
//            currentWatermark = new Watermark(currentTimestamp);
//        }
        return timestamp;
    }
}

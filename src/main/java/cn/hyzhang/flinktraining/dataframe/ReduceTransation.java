package cn.hyzhang.flinktraining.dataframe;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.walkthrough.common.entity.Transaction;

class ReduceTransation implements ReduceFunction<Transaction> {
    @Override
    public Transaction reduce(Transaction transaction, Transaction t1) throws Exception {
        transaction.setAmount(transaction.getAmount() + t1.getAmount());
        return transaction;
    }
}
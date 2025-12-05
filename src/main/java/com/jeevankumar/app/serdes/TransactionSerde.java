package com.jeevankumar.app.serdes;

import com.jeevankumar.app.event.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {

    public TransactionSerde() {
        super(new TransactionSerializer(), new TransactionDeserializer());
    }
}

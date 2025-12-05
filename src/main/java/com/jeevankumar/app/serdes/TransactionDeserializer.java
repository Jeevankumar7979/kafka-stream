package com.jeevankumar.app.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jeevankumar.app.event.Transaction;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeserializer implements Deserializer<Transaction> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String topic, byte[] bytes) {
        try {
            return mapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Transaction", e);
        }
    }
}

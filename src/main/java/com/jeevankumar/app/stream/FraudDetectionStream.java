package com.jeevankumar.app.stream;

import com.jeevankumar.app.event.Transaction;
import com.jeevankumar.app.serdes.TransactionSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class FraudDetectionStream {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FraudDetectionStream.class);

    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

        KStream<String, Transaction> transactions = builder
                .stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));
        transactions
                .filter((key, txn) -> txn.amount() > 10000)
                .peek((key, tx) -> log.warn("⚠️ FRAUD ALERT for {}", tx))
                .to("fraud-alerts", Produced.with(Serdes.String(), new TransactionSerde()));
        return transactions;

    }

}

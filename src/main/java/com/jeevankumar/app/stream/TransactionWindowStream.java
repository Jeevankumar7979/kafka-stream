package com.jeevankumar.app.stream;

import com.jeevankumar.app.event.Transaction;
import com.jeevankumar.app.serdes.TransactionSerde;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

@Configuration
@EnableKafkaStreams
@Log4j2
public class TransactionWindowStream {
    //source topic (transactions)
    //process (Windowing)10 > 3 -> fraud alert
    //write it back -> txn-fraud-alert

    @Bean
    public KStream<String, Transaction> windowedTransactionStream(StreamsBuilder builder) {

        KStream<String, Transaction> stream =
                builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        //u1- 5
        //u2- 3
        stream.groupBy((key, tx) -> tx.userId(),
                        Grouped.with(Serdes.String(), new TransactionSerde())
                ).windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.as("user-txn-count-window-store"))
                .toStream()
                .peek((windowedKey, count) -> {
                    String user = windowedKey.key();
                    log.info("🧾 User={} | Count={} | Window=[{} - {}]",
                            user,
                            count,
                            windowedKey.window().startTime(),
                            windowedKey.window().endTime());

                    if (count > 3) {
                        log.warn("🚨 FRAUD ALERT: User={} made {} transactions within 10 seconds!", user, count);
                    }
                })
                .to("user-txn-counts", Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(10).toMillis()),
                        Serdes.Long()
                ));
        return stream;

    }
}

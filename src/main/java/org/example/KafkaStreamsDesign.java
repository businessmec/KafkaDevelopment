package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsDesign {

    public final static String INPUT_TOPIC = "quickstart-events";

    public final static String OUTPUT_TOPIC = "outpout_topic";



    public void generateStreamKafka() {
        //   Stream

        Properties streamProperties = new Properties();
        StreamsBuilder streamsBuilder =new StreamsBuilder();

        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-config");
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "34.132.149.144:9092");
        streamProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1 );

        KStream<String, String> kStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> processedKStream = kStream.map((s, s2) -> KeyValue.pair(s + s2, s2 + s)).filter((o, o2) -> !"".equalsIgnoreCase(o) && !"".equalsIgnoreCase(o2));
        processedKStream.to(OUTPUT_TOPIC);
        Topology topology = streamsBuilder.build();
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties)) {

            final CountDownLatch shutdownLatch = new CountDownLatch(1);

//            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                System.out.println(" Stopping apache kafka stream ");
//                kafkaStreams.close(Duration.ofSeconds(2));
//                shutdownLatch.countDown();
//            }));

            try {
                System.out.println("starting apache kafka streams ");
                kafkaStreams.start();

                System.out.println("apache kafka streams started ");
                shutdownLatch.await();
            } catch (Throwable e) {
                System.out.println( "Erreur dans le topic " )
                ;
                e.printStackTrace();
                System.exit(1);
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }


    }

}

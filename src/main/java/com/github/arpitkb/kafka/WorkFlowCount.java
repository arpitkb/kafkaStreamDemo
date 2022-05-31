package com.github.arpitkb.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WorkFlowCount {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WorkFlowCount.class);

        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,"workflow-count-stream");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String,WorkFlow> input = builder.stream("workflows-input", Consumed.with(Serdes.String(), WorkflowSerde.WorkFlow()));

         KTable<String,Long> output = input
                 .selectKey((k,v)->v.getName())
                 .mapValues(val->val.getName() )
                         .groupByKey(Grouped.with(Serdes.String(),Serdes.String())).count();


         output.toStream().to("workflows-count" , Produced.with(Serdes.String(),Serdes.Long()));



        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);

        //start stream application
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            kafkaStreams.start();
            latch.await();
        }catch (final Throwable e){
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread("workflow-input-stream"){
            @Override
            public void run(){
                kafkaStreams.close();
                latch.countDown();
            }
        });

        System.exit(0);
    }
}

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
public class WorkFlowDetailed {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WorkFlowCount.class);

        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,"stat-stream");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,WorkFlow> input = builder.stream("workflows-input", Consumed.with(Serdes.String(), WorkflowSerde.WorkFlow()));


        // build topology
        KGroupedStream<String,WorkFlow> in2 = input.selectKey((k,v)->v.getName()).groupByKey(Grouped.with(Serdes.String(),WorkflowSerde.WorkFlow()));
        KTable<String,WorkFlowStat> output = in2.aggregate(
                ()-> new WorkFlowStat("temp",0L,0L,0L),
                (k,v,agg)-> {
                    if(v.getStatus().equals("Success")) agg.setSucc_count(agg.getSucc_count()+1);
                    else agg.setFail_count(agg.getFail_count()+1);
                    agg.setCount(agg.getCount()+1);
                    agg.setName(v.getName());
                    return agg;
                    },

                Materialized.with(Serdes.String(),WorkflowSerde.WorkFlowStat())
        );
        output.toStream().to("workflows-status",Produced.with(Serdes.String(),WorkflowSerde.WorkFlowStat()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);

        //start stream application
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            kafkaStreams.start();
            latch.await();
        }catch (final Throwable e){
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread("input-stream"){
            @Override
            public void run(){
                kafkaStreams.close();
                latch.countDown();
            }
        });

        System.exit(0);
    }
}

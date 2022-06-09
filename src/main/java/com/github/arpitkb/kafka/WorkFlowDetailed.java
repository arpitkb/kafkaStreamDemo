package com.github.arpitkb.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
public class WorkFlowDetailed {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(WorkFlowCount.class);
        final String inputTopic = "input-002";
        final String outputTopic = "output-001";

        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,"statStream");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,WorkFlow> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), WorkflowSerde.WorkFlow()));


        // build topology
        KStream<String,WorkFlow> in2 = input.filter((k, v)->v.getParentId() == null);
        KStream<String,WorkFlow> in3 = in2.merge(in2.flatMapValues(v->v.getNodes()));

        KGroupedStream<String,WorkFlow> in4 =  in3.selectKey((k,v)->v.getName()).groupByKey(Grouped.with(Serdes.String() , WorkflowSerde.WorkFlow()));

        KTable<String,WorkFlowStat> output = in4.aggregate(
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
        output.toStream().to(outputTopic,Produced.with(Serdes.String(),WorkflowSerde.WorkFlowStat()));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);

        //start stream application
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            kafkaStreams.start();
            latch.await();
        }catch (final Throwable e){
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread("inputStream"){
            @Override
            public void run(){
                kafkaStreams.close();
                latch.countDown();
            }
        });

        System.exit(0);
    }
}

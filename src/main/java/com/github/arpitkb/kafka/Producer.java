package com.github.arpitkb.kafka;

import com.github.arpitkb.kafka.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        String topic = "input-002";
        Logger logger = LoggerFactory.getLogger(Producer.class);


        Properties properties = new Properties();

        properties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        KafkaProducer<String,WorkFlow> producer=new KafkaProducer<>(properties);


         for(int i=0;i<9000;i++){
             int rand = 1+(int)Math.floor(Math.random()*15);
             int rand2 = (int)Math.floor(Math.random()*2);

             String name = "workflow "+rand;
             String status = rand2 == 0 ? "Success":"Failure";
             String id = "w"+rand+"-"+rand+".0";

            WorkFlow workFlow = new WorkFlow(id,name,status);

             int rand3 = 16+(int)Math.floor(Math.random()*8);
             for(int j=16;j<rand3;j++){
                 WorkFlow node = new WorkFlow("child_"+i ,"workflow "+j,"Success");
                 workFlow.addChild(node);
             }


            ProducerRecord<String,WorkFlow> record = new ProducerRecord<>(topic,workFlow.getName(),workFlow);

            producer.send(record,(recordMetadata, e) -> {
                if(e==null){
                    logger.info("topic : "+recordMetadata.topic()+" | Partition : "+recordMetadata.partition()+" | Offset : "+recordMetadata.offset());
                }else{
                    logger.error("Error while producing",e);
                }
            });
         }

        producer.flush();
        producer.close();
    }


}

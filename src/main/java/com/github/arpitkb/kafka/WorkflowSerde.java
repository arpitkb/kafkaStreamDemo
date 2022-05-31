package com.github.arpitkb.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class WorkflowSerde{

    private WorkflowSerde(){}

    public static Serde<WorkFlow> WorkFlow(){
        JsonSerializer<WorkFlow> serializer = new JsonSerializer<>();
        JsonDeserializer<WorkFlow> deserializer = new JsonDeserializer<>(WorkFlow.class);

        return Serdes.serdeFrom(serializer,deserializer);
    }

    public static Serde<WorkFlowStat> WorkFlowStat(){
        JsonSerializer<WorkFlowStat> serializer = new JsonSerializer<>();
        JsonDeserializer<WorkFlowStat> deserializer = new JsonDeserializer<>(WorkFlowStat.class);

        return Serdes.serdeFrom(serializer,deserializer);
    }
}

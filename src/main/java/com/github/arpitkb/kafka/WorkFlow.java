package com.github.arpitkb.kafka;

public class WorkFlow {
    private String id;
    private String name;
    private String status;

//    public WorkFlow() {}

    public WorkFlow(String id, String name, String status) {
        this.id = id;
        this.name = name;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getStatus() {
        return status;
    }

//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public void setStatus(String status) {
//        this.status = status;
//    }
}

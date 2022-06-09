package com.github.arpitkb.kafka;

import java.util.ArrayList;

public class WorkFlow {
    private String id;

    private String parent_id;
    private String name;
    private String status;

    private ArrayList<WorkFlow> nodes;

   public WorkFlow() {}

    public WorkFlow(String id, String name, String status) {
        this.parent_id = null;
        this.id = id;
        this.name = name;
        this.status = status;
        nodes=new ArrayList<>();
    }

    public void addChild(WorkFlow a){
        a.parent_id = id;
        nodes.add(a);
    }

    public String getId() {
        return id;
    }

    public ArrayList<WorkFlow> getNodes(){
        return nodes;
    }

    public String getParentId() {
        return parent_id;
    }

    public String getName() {
        return name;
    }

    public String getStatus() {
        return status;
    }

   public void setId(String id) {
       this.id = id;
   }

   public void setName(String name) {
       this.name = name;
   }

   public void setStatus(String status) {
       this.status = status;
   }
}

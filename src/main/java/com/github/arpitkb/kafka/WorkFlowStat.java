package com.github.arpitkb.kafka;

public class WorkFlowStat {

    private String name;
    private long succ_count;
    private long fail_count;
    private long count;

    public WorkFlowStat(String name, long succ_count, long fail_count, long count) {
        this.name = name;
        this.succ_count = succ_count;
        this.fail_count = fail_count;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSucc_count() {
        return succ_count;
    }

    public void setSucc_count(long succ_count) {
        this.succ_count = succ_count;
    }

    public long getFail_count() {
        return fail_count;
    }

    public void setFail_count(long fail_count) {
        this.fail_count = fail_count;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}

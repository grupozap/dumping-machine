package com.grupozap.dumping_machine.partitioners;

class PartitionInfo {
    private int partition;
    private long offset;

    public PartitionInfo(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}

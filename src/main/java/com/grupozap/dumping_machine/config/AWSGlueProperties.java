package com.grupozap.dumping_machine.config;

public class AWSGlueProperties {

    private String recordTable;

    private String errorTable;

    private String tombstoneTable;

    private String region;

    public String getRecordTable() {
        return recordTable;
    }

    public void setRecordTable(String recordTable) {
        this.recordTable = recordTable;
    }

    public String getErrorTable() {
        return errorTable;
    }

    public void setErrorTable(String errorTable) {
        this.errorTable = errorTable;
    }

    public String getTombstoneTable() {
        return tombstoneTable;
    }

    public void setTombstoneTable(String tombstoneTable) {
        this.tombstoneTable = tombstoneTable;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}

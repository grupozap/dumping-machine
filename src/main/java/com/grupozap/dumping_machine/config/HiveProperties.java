package com.grupozap.dumping_machine.config;

public class HiveProperties {
    private String url;
    private String user;
    private String password;
    private String recordTable;
    private String errorTable;
    private String tombstoneTable;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

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
}

package com.grupozap.dumping_machine.metastore;

import java.util.List;

public interface MetastoreClient<T> {

    boolean databaseExists(String database) throws Exception;

    void createDatabase(String database) throws Exception;

    boolean tableExists(String database, String tableName) throws Exception;

    <T> List<T> getSchemaTable(String database, String table) throws Exception;

    void createTable(String database, String table, List<T> columns, List<T> partitions, String location) throws Exception;

    void alterTable(String database, String tableName, List<T> columns) throws Exception;

    boolean compareSchemas(List<T> schema, List<T> newSchema);

    void addPartition(String database, String tableName, String partition) throws Exception;

    void close();
}

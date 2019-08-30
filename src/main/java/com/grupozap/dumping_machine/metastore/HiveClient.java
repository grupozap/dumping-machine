package com.grupozap.dumping_machine.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveClient {

    private final IMetaStoreClient client;
    private final Logger logger = LoggerFactory.getLogger(HiveClient.class);
    private final static String OWNER_NAME = "root";

    public HiveClient(String metaStoreUris) throws Exception {

        if(metaStoreUris == null || metaStoreUris.equals(""))
            throw new IllegalArgumentException("hive.metastore.uris is not defined!");

        Configuration conf = new Configuration();
        HiveConf hiveConf = new HiveConf(conf, HiveClient.class);
        hiveConf.set("hive.metastore.uris", metaStoreUris);

        this.client = new HiveMetaStoreClient(hiveConf);
    }

    public void createTable(String database, String tableName, List<FieldSchema> columns, List<FieldSchema> partitions, String location) throws Exception {
        Table table = getStructureTable(database, tableName, columns, partitions, location);

        try {
            client.createTable(table.getTTable());
        } catch (AlreadyExistsException ex) {
            logger.warn("Table already exists: {}.{}", database, tableName);
        }
    }

    public void alterTable(String database, String tableName, List<FieldSchema> columns) throws Exception {
        Table table = new Table(client.getTable(database, tableName));
        table.setFields(columns);

        client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
    }

    public List<FieldSchema> getSchemaTable(String database, String tableName) throws Exception {
        return new Table(client.getTable(database, tableName)).getAllCols();
    }

    public void createDatabase(String database) throws Exception {
        Database db = new Database(database, "database created by Dumping machine", null, null);
        db.setOwnerName(OWNER_NAME);

        try {
            client.createDatabase(db);
        } catch (AlreadyExistsException ex) {
            logger.warn("Database already exists: {}", database);
        }
    }

    public boolean tableExists(String database, String tableName) throws Exception {
        return client.tableExists(database, tableName);
    }

    public boolean databaseExists(String database) throws Exception {
        try {
            client.getDatabase(database);
        } catch (NoSuchObjectException ex) {
            return false;
        }
        return true;
    }

    public boolean compareSchemas(List<FieldSchema> schema, List<FieldSchema> newSchema) {
        return MetaStoreUtils.compareFieldColumns(schema, newSchema);
    }

    public void addPartition(String database, String tableName, String partition) throws Exception{
        try {
            client.appendPartition(database, tableName, partition);
        } catch (AlreadyExistsException ex) {
            logger.warn("Partition {} already exists in table: {}.{}", partition, database, tableName);
        }
    }

    public void close(){
        client.close();
    }

    private Table getStructureTable(String database, String tableName, List<FieldSchema> columns, List<FieldSchema> partitions, String location) throws HiveException {
        Table table = new Table(database, tableName);

        table.setTableType(TableType.EXTERNAL_TABLE);
        table.setOwner(OWNER_NAME);
        table.getParameters().put("EXTERNAL", "TRUE");
        table.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        table.setInputFormatClass("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        table.setOutputFormatClass("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

        table.setDataLocation(new Path(location));
        table.setFields(columns);
        table.setPartCols(partitions);

        return table;
    }
}
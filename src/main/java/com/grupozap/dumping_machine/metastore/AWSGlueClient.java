package com.grupozap.dumping_machine.metastore;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AWSGlueClient implements MetastoreClient<Column> {

    private final Logger logger = LoggerFactory.getLogger(AWSGlueClient.class);
    private final AWSGlue client;

    public AWSGlueClient(String region) {
        this.client = AWSGlueClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
    }

    public boolean compareSchemas(List<Column> schema, List<Column> newSchema) {
        return schema.equals(newSchema);
    }

    public void alterTable(String database, String tableName, List<Column> columns) {
        Table table = this.client.getTable(new GetTableRequest().withDatabaseName(database).withName(tableName))
                .getTable();

        TableInput tableInput = new TableInput();
        tableInput.setStorageDescriptor(table.getStorageDescriptor().withColumns(columns));
        tableInput.setParameters(table.getParameters());
        tableInput.setPartitionKeys(table.getPartitionKeys());
        tableInput.setName(table.getName());
        tableInput.setTableType(table.getTableType());

        this.client.updateTable(new UpdateTableRequest().withDatabaseName(database).withTableInput(tableInput));
    }

    public boolean tableExists(String database, String table) {
        try {
            this.client.getTable(new GetTableRequest().withDatabaseName(database).withName(table));
        } catch(EntityNotFoundException enfe) {
            return false;
        }
        return true;
    }

    private TableInput getStructureTable(String tableName, List<Column> columns, List<Column> partitions, String location) {
        Map tableParameters = new HashMap<String, String>();
        tableParameters.put("classification", "parquet");
        tableParameters.put("parquet.compress", "SNAPPY");

        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setLocation(location.replace("s3a://", "s3://"));
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo().withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        Map serdeParameters = new HashMap<String, String>();
        serdeParameters.put("serialization.format", "1");
        serDeInfo.setParameters(serdeParameters);

        storageDescriptor.setSerdeInfo(serDeInfo);
        storageDescriptor.setColumns(columns);

        TableInput tableInput = new TableInput();
        tableInput.setName(tableName);
        tableInput.setStorageDescriptor(storageDescriptor);
        tableInput.setTableType("EXTERNAL"); // ??
        tableInput.setParameters(tableParameters);
        tableInput.setPartitionKeys(partitions);

        return tableInput;
    }

    public boolean databaseExists(String database) {
        try {
            this.client.getDatabase(new GetDatabaseRequest().withName(database));
        } catch(EntityNotFoundException enfe) {
            return false;
        }
        return true;
    }

    public List<Column> getSchemaTable(String database, String tableName) {
        Table table = this.client.getTable(new GetTableRequest().withDatabaseName(database).withName(tableName))
                .getTable();
        List<Column> columns = table.getStorageDescriptor().getColumns();
        columns.addAll(table.getPartitionKeys());

        return columns;
    }

    public void createDatabase(String database) {
        try {
            this.client.createDatabase(
                    new CreateDatabaseRequest().withDatabaseInput(new DatabaseInput().withName(database)));
        } catch (AlreadyExistsException ex) {
            logger.warn("Database already exists: {}", database);
        }

    }

    public void createTable(String database, String table, List<Column> columns, List<Column> partitions, String location) {
        TableInput tableInput = getStructureTable(table, columns, partitions, location);

        this.client.createTable(
                new CreateTableRequest()
                        .withDatabaseName(database)
                        .withTableInput(tableInput));
    }

    public void addPartition(String database, String tableName, String partition) {
        try {

            Table table = this.client.getTable(new GetTableRequest().withDatabaseName(database).withName(tableName))
                    .getTable();
            StorageDescriptor storageDescriptor = table.getStorageDescriptor();
            storageDescriptor.setLocation(storageDescriptor.getLocation().replace("s3a://", "s3://") + partition);

            List<String> partitions = new ArrayList();
            Pattern p = Pattern.compile("\\w+=([^\\/]+)");
            Matcher m = p.matcher(partition);

            while(m.find()){
                partitions.add(m.group(1));
            }

            PartitionInput pi = new PartitionInput().withValues(partitions).withStorageDescriptor(storageDescriptor);

            this.client.createPartition(new CreatePartitionRequest()
                    .withDatabaseName(database)
                    .withTableName(tableName)
                    .withPartitionInput(pi)
            );
        } catch (AlreadyExistsException ex) {
            logger.warn("Partition {} already exists in table: {}.{}", partition, database, tableName);
        }
    }

    public void close() {
        this.client.shutdown();
    }

}

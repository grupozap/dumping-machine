package com.grupozap.dumping_machine.metastore;

import org.apache.avro.Schema;
import com.grupozap.dumping_machine.deserializers.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetastoreService {

    private static final Logger logger = LoggerFactory.getLogger(MetastoreService.class);
    private final MetastoreClient metastoreClient;
    private final AvroToMetastore avroToMetastore;
    private final Map<RecordType, String> tables;

    public MetastoreService(Map<RecordType, String> tables, MetastoreClient metastoreClient, AvroToMetastore avroToMetastore) {
        this.tables = tables;
        this.metastoreClient = metastoreClient;
        this.avroToMetastore = avroToMetastore;
    }

    public void updateMetastore(Object key, Schema schemaAvro, String path, String serverPath, String partitionPattern) throws Exception {
        String table = this.tables.getOrDefault(key, null);

        if(table != null) {
            String[] tableSplit = table.split("\\.");
            String dataBase = tableSplit[0];
            String tableName = tableSplit[1];
            String location = getLocation(path, serverPath);
            String partition = getPartition(path);

            try {
                if (this.metastoreClient.tableExists(dataBase, tableName))
                    updateTable(dataBase, tableName, schemaAvro, partitionPattern);
                else
                    createTable(dataBase, tableName, schemaAvro, location, partitionPattern);

                logger.info("Adding partition {} in table {}.{}", partition, dataBase, tableName);
                this.metastoreClient.addPartition(dataBase, tableName, partition);
            } finally {
                this.metastoreClient.close();
            }
        }
    }

    private void updateTable(String dataBase, String tableName, Schema schemaAvro, String partitionPattern) throws Exception {
        List newSchema = this.avroToMetastore.generateSchema(schemaAvro);
        List oldSchema = this.metastoreClient.getSchemaTable(dataBase, tableName);

        List newSchemaCompare = new ArrayList(newSchema);
        newSchemaCompare.addAll(this.avroToMetastore.getPartitions(partitionPattern));

        if (!this.metastoreClient.compareSchemas(oldSchema, newSchemaCompare)) {
            logger.info("Updating table {}.{}", dataBase, tableName);
            this.metastoreClient.alterTable(dataBase, tableName, newSchema);
        }
    }

    private void createTable(String dataBase, String tableName, Schema schemaAvro, String location, String partitionPattern) throws Exception {
        List schema = this.avroToMetastore.generateSchema(schemaAvro);
        List partitions = this.avroToMetastore.getPartitions(partitionPattern);

        if (!this.metastoreClient.databaseExists(dataBase)) {
            logger.info("Creating database {}", dataBase);
            this.metastoreClient.createDatabase(dataBase);
        }

        logger.info("Creating table {}.{}", dataBase, tableName);
        this.metastoreClient.createTable(dataBase, tableName, schema, partitions, location);
    }

    private static String getPartition(String path) {
        String[] key_split = path.split("/");

        if(key_split.length == 4) {
            return key_split[2] + "/";
        } else {
            return key_split[2] + "/" + key_split[3] + "/";
        }

    }

    private static String getLocation(String path, String serverPath) {
        String[] key_split = path.split("/");

        return serverPath + "/" +  key_split[0] + "/" + key_split[1] + "/";
    }
}

package com.grupozap.dumping_machine.metastore;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class HiveUtil {

    private static final Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static void updateHive(HiveClient hiveClient, String table, Schema schemaAvro, String path, String serverPath, String partitionPattern) throws Exception {

        String[] tableSplit = table.split("\\.");
        String dataBase = tableSplit[0];
        String tableName = tableSplit[1];
        String location = getLocation(path, serverPath);
        String partition = getPartition(path);

        try {
            if (hiveClient.tableExists(dataBase, tableName))
                updateTable(hiveClient, dataBase, tableName, schemaAvro, partitionPattern);
            else
                createTable(hiveClient, dataBase, tableName, schemaAvro, location, partitionPattern);

            logger.info("Adding partition {} in table {}.{}", partition, dataBase, tableName);
            hiveClient.addPartition(dataBase, tableName, partition);
        } finally {
            hiveClient.close();
        }
    }

    private static void updateTable(HiveClient hiveClient, String dataBase, String tableName, Schema schemaAvro, String partitionPattern) throws Exception {
        List<FieldSchema> newSchema = AvroToHive.generateSchema(schemaAvro);
        List<FieldSchema> oldSchema = hiveClient.getSchemaTable(dataBase, tableName);

        List<FieldSchema> newSchemaCompare = new ArrayList<>(newSchema);
        newSchemaCompare.addAll(AvroToHive.getPartitions(partitionPattern));

        if (!hiveClient.compareSchemas(oldSchema, newSchemaCompare)) {
            logger.info("Updating table {}.{}", dataBase, tableName);
            hiveClient.alterTable(dataBase, tableName, newSchema);
        }
    }

    private static void createTable(HiveClient hiveClient, String dataBase, String tableName, Schema schemaAvro, String location, String partitionPattern) throws Exception {
        List<FieldSchema> schema = AvroToHive.generateSchema(schemaAvro);
        List<FieldSchema> partitions = AvroToHive.getPartitions(partitionPattern);

        if (!hiveClient.databaseExists(dataBase)) {
            logger.info("Creating database {}", dataBase);
            hiveClient.createDatabase(dataBase);
        }

        logger.info("Creating table {}.{}", dataBase, tableName);
        hiveClient.createTable(dataBase, tableName, schema, partitions, location);
    }

    private static String getPartition(String path) {
        String[] key_split = path.split("/");

        return key_split[2] + "/" + key_split[3] + "/";
    }

    private static String getLocation(String path, String serverPath) {
        String[] key_split = path.split("/");

        return serverPath + "/" +  key_split[0] + "/" + key_split[1] + "/";
    }
}

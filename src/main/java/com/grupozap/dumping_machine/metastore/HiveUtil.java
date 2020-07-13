package com.grupozap.dumping_machine.metastore;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.lang.System;

public abstract class HiveUtil {

    private static final Logger logger = LoggerFactory.getLogger(HiveUtil.class);

    public static void updateHive(HiveClient hiveClient, String table, Schema schemaAvro, String path, String serverPath) throws Exception {

        String[] tableSplit = table.split("\\.");
        String dataBase = tableSplit[0];
        String tableName = tableSplit[1];
        String location = getLocation(path, serverPath);
        String partition = getPartition(path);

        try {
            if (hiveClient.tableExists(dataBase, tableName))
                updateTable(hiveClient, dataBase, tableName, schemaAvro);
            else
                createTable(hiveClient, dataBase, tableName, schemaAvro, location);

            logger.info("Adding partition {} in table {}.{}", partition, dataBase, tableName);
            hiveClient.addPartition(dataBase, tableName, partition);
        } finally {
            hiveClient.close();
        }
    }

    private static void updateTable(HiveClient hiveClient, String dataBase, String tableName, Schema schemaAvro) throws Exception {
        List<FieldSchema> newSchema = AvroToHive.generateSchema(schemaAvro);
        List<FieldSchema> oldSchema = hiveClient.getSchemaTable(dataBase, tableName);

        List<FieldSchema> newSchemaCompare = new ArrayList<>(newSchema);
        newSchemaCompare.addAll(AvroToHive.getPartitions());

        if (!hiveClient.compareSchemas(oldSchema, newSchemaCompare)) {

            if(Boolean.parseBoolean(System.getenv("DM_HIVE_DISABLE_UPDATE_COLUMN_COMMENT")))
                getHiveColumnsComments(oldSchema, newSchema);

            logger.info("Updating table {}.{}", dataBase, tableName);
            hiveClient.alterTable(dataBase, tableName, newSchema);
        }
    }

    private static void createTable(HiveClient hiveClient, String dataBase, String tableName, Schema schemaAvro, String location) throws Exception {
        List<FieldSchema> schema = AvroToHive.generateSchema(schemaAvro);
        List<FieldSchema> partitions = AvroToHive.getPartitions();

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

    private static void getHiveColumnsComments(List<FieldSchema> oldFields, List<FieldSchema> newFields) {

        for (FieldSchema newField : newFields) {
            for (FieldSchema oldField : oldFields) {
                if (newField.getName().equals(oldField.getName())) {
                    newField.setComment(oldField.getComment());
                    break;
                } else {
                    newField.unsetComment();
                }
            }
        }
    }
}

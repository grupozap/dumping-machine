package com.grupozap.dumping_machine.metastore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveClient {
    private final Logger logger = LoggerFactory.getLogger(HiveClient.class);
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private final String urlHive;
    private final String userHive;
    private final String passwordHive;

    public HiveClient(String urlHive, String userHive, String passwordHive) throws Exception {
        this.urlHive = urlHive;
        this.userHive = userHive;
        this.passwordHive = passwordHive;

        try {
            Class.forName(driverName);
        } catch (Exception ex) {
            logger.error("Error load drive hive");
            ex.printStackTrace();
            throw ex;
        }
    }

    public void addPartition(String table, String location, String partition) throws Exception {

        String query = "ALTER TABLE " + table + " ADD IF NOT EXISTS PARTITION (" + partition + ") LOCATION '" + location + "'";

        try (Connection connection = DriverManager.getConnection(this.urlHive, this.userHive, this.passwordHive)) {
            Statement statement = connection.createStatement();

            statement.execute(query);
            logger.info("Added partition: " + location + " in hive table: " + table);

        } catch(Exception ex){
            logger.error("Error adding partition " +  location + " in hive table: " + table);
            throw ex;
        }
    }
}

package com.grupozap.dumping_machine;

import com.grupozap.dumping_machine.config.ApplicationProperties;
import com.grupozap.dumping_machine.streamers.KafkaStreamer;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

class Application {
    public static void main(String[] args) {
        String properties = System.getProperty("config");

        if( properties == null ) {
            System.out.println( "Usage: -Dconfig=<file.yml>" );
            return;
        }

        ApplicationProperties applicationProperties = null;
        Yaml yaml = new Yaml();

        try( InputStream in = Files.newInputStream( Paths.get( properties ) ) ) {
            applicationProperties = yaml.loadAs( in, ApplicationProperties.class );
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaStreamer kafkaStreamer = new KafkaStreamer(applicationProperties);
        kafkaStreamer.run();
    }
}

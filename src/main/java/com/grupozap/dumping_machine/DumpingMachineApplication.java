package com.grupozap.dumping_machine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
class DumpingMachineApplication {

    public static void main(String[] args) {
        try {
            SpringApplication.run(DumpingMachineApplication.class, args);
        } catch (Throwable e) {
            System.out.println(e.getStackTrace());
        }
    }
}

package com.grupozap.dumping_machine.uploaders;

public interface Uploader {
    void upload(String remotePath, String filename) throws Exception;

    String getServerPath();
}

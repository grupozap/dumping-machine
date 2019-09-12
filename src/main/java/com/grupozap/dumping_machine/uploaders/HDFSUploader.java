package com.grupozap.dumping_machine.uploaders;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class HDFSUploader implements Uploader {
    private final String coreSitePath;
    private final String hdfsSitePath;
    private final String hdfsPath;
    private String topicPath;

    public HDFSUploader(String hdfsPath, String coreSitePath, String hdfsSitePath, String topicPath) {
        this.coreSitePath = coreSitePath;
        this.hdfsSitePath = hdfsSitePath;
        this.hdfsPath = hdfsPath;
        this.topicPath = topicPath;
    }

    public void upload(String remotePath, String filename) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path(this.coreSitePath));
        conf.addResource(new Path(this.hdfsSitePath));

        FileSystem fs = FileSystem.get(URI.create(this.hdfsPath), conf);

        Path hdfsWritePath = new Path(topicPath + remotePath);

        FSDataOutputStream os = fs.create(hdfsWritePath);
        InputStream is = new BufferedInputStream(new FileInputStream(filename));

        try {
            IOUtils.copyBytes(is, os, conf);
        } finally {
            IOUtils.closeStream(os);
            IOUtils.closeStream(is);
        }
    }

    public String getServerPath() {
        return  hdfsPath + topicPath.replace("/", "");
    }
}

package com.swarup.hadoop.learning.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * Created by swaroop on 21/12/16.
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws IOException {
        if(args.length != 2) {
            System.err.println("Need 2 parameter(inputFile,destFile) for the program!");
        } else {
            String inputUri = args[0];
            String outputUri = args[1];
            FileSystem fileSystem = FileSystem.get(URI.create(inputUri), new Configuration());
            FSDataInputStream fsDataInputStream = null;
            FSDataOutputStream fsDataOutputStream = null;
            try {
                fsDataInputStream = fileSystem.open(new Path(inputUri));
                fsDataOutputStream = fileSystem.create(new Path(outputUri), new Progressable() {
                    @Override
                    public void progress() {
                        System.out.println("#");
                    }
                });
                IOUtils.copyBytes(fsDataInputStream,fsDataOutputStream, 4096, false);
            } finally {
                IOUtils.closeStream(fsDataInputStream);
                IOUtils.closeStream(fsDataOutputStream);
            }
        }
    }
}

package com.swarup.hadoop.learning.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by swaroop on 21/12/16.
 */
public class FileSystemCat {

    public static void main(String[] args) throws IOException {
        if(args.length != 1) {
            System.err.println("Need the file name to cat the output!");
        } else {
            String uri = args[0];
            FileSystem fileSystem = FileSystem.get(URI.create(uri), new Configuration());
            InputStream inputStream = null;
            try {
                inputStream = fileSystem.open(new Path(uri));
                IOUtils.copyBytes(inputStream, System.out, 4096, false);
            } finally {
                IOUtils.closeStream(inputStream);
            }
        }
    }
}

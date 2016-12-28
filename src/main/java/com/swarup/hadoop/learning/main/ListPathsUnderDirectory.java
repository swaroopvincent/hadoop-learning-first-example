package com.swarup.hadoop.learning.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by swaroop on 22/12/16.
 */
public class ListPathsUnderDirectory {

    public static void main(String[] args) throws IOException {

        final String uri = args[0];
        FileSystem fileSystem = FileSystem.get(URI.create(uri), new Configuration());
        Path[] paths = new Path[args.length];
        for(int i=0; i<paths.length; i++) {
            paths[i] = new Path(args[i]);
        }
        FileStatus[] statuses = fileSystem.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(statuses);
        for(Path path : listedPaths) {
            System.out.println(path);
        }
    }
}

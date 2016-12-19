package com.swarup.hadoop.learning.main;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by swaroop on 19/12/16.
 */
public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 1) {
            System.err.println("Need file to cat the output.");
        }
        else {
            InputStream in = null;
            try {
                in = new URL(args[0]).openStream();
                IOUtils.copyBytes(in, System.out, 4096, false);
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }
}

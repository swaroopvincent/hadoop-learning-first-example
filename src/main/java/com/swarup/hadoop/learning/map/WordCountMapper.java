package com.swarup.hadoop.learning.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by swaroop on 12/12/16.
 */
public class WordCountMapper extends Mapper<LongWritable,Text, Text,IntWritable>{

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        for(String word: line.toString().split(" ")) {
            context.write(new Text(word), ONE);
        }
    }

}

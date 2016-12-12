package com.swarup.hadoop.learning.main;

import com.swarup.hadoop.learning.map.WordCountMapper;
import com.swarup.hadoop.learning.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by swaroop on 12/12/16.
 */
public class WordCountTask {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length !=2) {
            System.err.println("Usage: WordCount <input file path> <output file path>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(WordCountTask.class);
        job.setJobName("Word count");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

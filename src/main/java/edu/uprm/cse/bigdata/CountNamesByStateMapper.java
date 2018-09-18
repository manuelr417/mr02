package edu.uprm.cse.bigdata;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountNamesByStateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //StringTokenizer strTok = new StringTokenizer(value.toString(), ",");
        String cols[] = value.toString().split(",");
        // get the state name, located in column
        String state = cols[0];
        String name = cols[3];
        String newKey = state + "-" + name;
        context.write(new Text(state), new IntWritable(1));

    }
}

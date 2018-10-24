package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OcurrancesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String idList = ""

        for (Text value : values ){
            idList += ", " + value.toString();
        }
        context.write(key, new Text(idList));
    }

}

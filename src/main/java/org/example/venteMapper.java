package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class venteMapper extends Mapper<LongWritable, Text , Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

        String[] ventes = value.toString().split(" ");
        String ville = ventes[1];
        double prix = Double.parseDouble(ventes[3]);
        context.write(new Text(ville), new DoubleWritable(prix));
    }
}

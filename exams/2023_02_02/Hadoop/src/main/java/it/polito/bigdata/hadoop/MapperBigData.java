package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

        private int threshold = 60;
        
        protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
        
                /* Implement the map method */ 
                String[] line = value.toString().split(",");
                if(Integer.parseInt(line[3]) < threshold){
                        context.write(new Text(line[1]), new IntWritable(1));
                }
        }
}

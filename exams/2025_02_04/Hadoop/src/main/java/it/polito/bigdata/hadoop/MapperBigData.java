package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        Text> { // Output value type
        
        protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
        
                        /* Implement the map method */ 
                        // users : User1000,Mario,Rossi,Italian,Business
                        String[] line = value.toString().split(",");
                        if (line[4].compareTo("free") == 0){
                                context.write(new Text(line[3]), new Text("1,0"));
                        } else {
                                context.write(new Text(line[3]), new Text("0,1"));
                        }
        }
}

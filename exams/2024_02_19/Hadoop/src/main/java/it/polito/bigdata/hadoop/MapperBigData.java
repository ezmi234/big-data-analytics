package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        NullWritable> { // Output value type
        
        protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
        
                        /* Implement the map method */ 
                        // 2019/02/02-09:15:01,User20,ID1,50.99
                        String userID = value.toString().split(",")[1];
                        String productId = value.toString().split(",")[2];
                        context.write(new Text(userID+","+productId), NullWritable.get());
        }
}

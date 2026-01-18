package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        NullWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type
        
        @Override
        protected void reduce(
            Text key, // Input key type
            Iterable<NullWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
            
            /* Implement the reduce method */
            String userID = key.toString().split(",")[0];
            context.write(new Text(userID), new IntWritable(1));
        }

}

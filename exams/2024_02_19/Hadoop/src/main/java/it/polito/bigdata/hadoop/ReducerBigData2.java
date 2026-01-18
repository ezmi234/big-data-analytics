package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer second job
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,    // Output key type
                NullWritable> {  // Output value type
                private int threshold = 50;
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
            int count = 0;
        /* Implement the reduce method */
        for (IntWritable intWritable : values) {
            count += intWritable.get();
        }

        if(count >= threshold){
            context.write(key, NullWritable.get());
        }
    }
}

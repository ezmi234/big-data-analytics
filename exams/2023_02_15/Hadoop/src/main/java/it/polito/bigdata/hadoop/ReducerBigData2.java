package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer second job
 */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,    // Output key type
                DoubleWritable> {  // Output value type
                private double threshold = 1000;

    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
        /* Implement the reduce method */
        int n = 0;
        int sum = 0;
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
            n++;
        }
        double avg = sum/n;
        if(avg > threshold){
            context.write(key, new DoubleWritable(avg));
        }
    }
}

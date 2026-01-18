package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type
        
        @Override
        protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {
            
            /* Implement the reduce method */
            int countFree = 0;
            int countNotFree = 0;
            for (Text text : values) {
                int free = Integer.parseInt(text.toString().split(",")[0]);
                int not_free = Integer.parseInt(text.toString().split(",")[1]);
                countFree += free;
                countNotFree += not_free;
            }

            if(countNotFree == 0){
                context.write(key, new IntWritable(countFree));
            }
        }

}

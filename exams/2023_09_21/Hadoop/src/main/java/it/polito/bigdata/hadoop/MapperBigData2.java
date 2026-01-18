package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper second job
 */
class MapperBigData2 extends Mapper<
                    Text,  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    Text> {		  // Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	/* Implement the map method */ 
        String seasonId = key.toString().split("_")[0];
        context.write(new Text(seasonId), new Text(value.toString()));
    }
}

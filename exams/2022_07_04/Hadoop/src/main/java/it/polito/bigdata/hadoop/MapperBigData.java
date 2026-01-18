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
        private String startingDate = "2021/07/04";
        private String endigDate = "2022/07/03";
        
        protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
                
                String[] line = value.toString().split(",");
                if(line[1].compareTo(endigDate) <=0 && line[1].compareTo(startingDate) >= 0){
                        context.write(new Text(line[2]), new IntWritable(1));
                }
                
        }
}

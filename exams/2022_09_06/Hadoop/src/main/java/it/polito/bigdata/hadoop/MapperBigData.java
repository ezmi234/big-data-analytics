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
        ConsumptionPerYear> { // Output value type

        private double threshlod = 1000;
        
        protected void map(
                LongWritable key,   // Input key type
                Text value,         // Input value type
                Context context) throws IOException, InterruptedException {
        
                /* Implement the map method */ 
                String[] line = value.toString().split(",");
                if(Double.parseDouble(line[2]) > threshlod){
                        ConsumptionPerYear consumptionPerYear = new ConsumptionPerYear();
                        if(Integer.parseInt(line[1].split("/")[0]) == 2020)
                                consumptionPerYear.setYear2020(1);
                        if(Integer.parseInt(line[1].split("/")[0]) == 2021)
                                consumptionPerYear.setYear2021(1);
                        context.write(new Text(line[0]), consumptionPerYear);
                }
        }
}

package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        ConsumptionPerYear, // Input value type
        Text, // Output key type
        NullWritable> { // Output value type
        
        @Override
        protected void reduce(
            Text key, // Input key type
            Iterable<ConsumptionPerYear> values, // Input value type
            Context context) throws IOException, InterruptedException {

            int count2020 = 0;
            int count2021 = 0;
            /* Implement the reduce method */
            for (ConsumptionPerYear consumptionPerYear : values) {
                count2020 += consumptionPerYear.getYear2020();
                count2021 += consumptionPerYear.getYear2021();
            }

            if(count2021 > count2020){
                context.write(key, null);
            }
        }
    

}

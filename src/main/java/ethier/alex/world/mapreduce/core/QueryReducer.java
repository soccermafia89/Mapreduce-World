/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.data.ElementListWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryReducer extends Reducer<Text, BigDecimalWritable, Text, Text> {
        
    private static Logger logger = Logger.getLogger(QueryReducer.class);
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) {

        logger.info("Reducer setup finished.");
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<BigDecimalWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Reducing Key: " + key.toString());

        logger.info("TODO");
    }
}

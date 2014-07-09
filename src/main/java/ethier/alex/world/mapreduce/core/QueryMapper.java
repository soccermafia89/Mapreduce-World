/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.mapreduce.data.PartitionWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryMapper extends Mapper<Text, ElementListWritable, Text, Writable> {
    
    private static Logger logger = Logger.getLogger(QueryMapper.class);
    
    @Override
    protected void setup(Context context) {
        
        int mapperId = context.getTaskAttemptID().getTaskID().getId();
        logger.info("Setting up mapper: " + mapperId);
        logger.info("Setup complete.");
    }

    @Override
    public void map(Text key, ElementListWritable value, Context context) {
        
        ElementList elementList = value.getElementList();
        
        
        
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        outputWriter.close();
    }
}

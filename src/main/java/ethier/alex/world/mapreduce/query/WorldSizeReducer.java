/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.memory.TaskMemoryManager;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldSizeReducer extends Reducer<Text, BigDecimalWritable, Text, Text> {
    
    private static Logger logger = Logger.getLogger(WorldSizeReducer.class);
    
    private TaskMemoryManager memoryManager;
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
        
        memoryManager = new TaskMemoryManager(context);
        logger.info("Reducer setup finished.");
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<BigDecimalWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Reducing Key: " + key.toString());
        
        BigDecimal sum = new BigDecimal(0L);
        
        Iterator<BigDecimalWritable> it = values.iterator();
        while(it.hasNext()) {
            BigDecimal bigDecimal = it.next().getBigDecimal();
            sum = sum.add(bigDecimal);
        }

        memoryManager.setString(WorldSizeRunner.MEMORY_WORLD_SIZE_NAME, sum.toPlainString());
    }
}

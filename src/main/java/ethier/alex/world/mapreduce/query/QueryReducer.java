/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.mapreduce.memory.MemoryManager;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.memory.TaskMemoryManager;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryReducer extends Reducer<Text, BigDecimalWritable, Text, BigDecimalWritable> {
        
    private static Logger logger = Logger.getLogger(QueryReducer.class);
    BigDecimal worldSize;
    private TaskMemoryManager memoryManager;
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
        
        memoryManager = new TaskMemoryManager(context);
        Map<String, String> memoryMap = memoryManager.syncMemory();

        String worldSizeStr = memoryMap.get(WorldSizeRunner.MEMORY_WORLD_SIZE_NAME);
        worldSize = new BigDecimal(worldSizeStr);
        logger.info("Reducer setup finished.");
    }

    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<BigDecimalWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("Reducing Key: " + key.toString());

        Iterator<BigDecimalWritable> it = values.iterator();
        BigDecimal sum = new BigDecimal(0L);
        while(it.hasNext()) {
            BigDecimal weight = it.next().getBigDecimal();
            sum = sum.add(weight);
        }
        
        BigDecimal probability = sum.divide(worldSize, 10, RoundingMode.UP);
                
        memoryManager.setString(QueryRunner.MEMORY_QUERY_NAME, probability.toPlainString());
        
        context.write(key, new BigDecimalWritable(probability));
    }
}

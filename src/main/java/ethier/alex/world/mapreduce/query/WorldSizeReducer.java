/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.mapreduce.memory.HdfsMemoryManager;
import ethier.alex.world.mapreduce.memory.MemoryToken;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldSizeReducer extends Reducer<Text, BigDecimalWritable, Text, Text> {
    
    private static Logger logger = Logger.getLogger(WorldSizeReducer.class);
    
//    private String outputPath;
//    private HdfsMemoryManager memoryManager;
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
        
//        memoryManager = HdfsMemoryManager.openManager(context.getConfiguration());
//        outputPath = context.getConfiguration().get(WorldSizeRunner.WORLD_SIZE_OUTPUT_KEY);
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

        HdfsMemoryManager.setString(WorldSizeRunner.MEMORY_WORLD_SIZE_NAME, sum.toPlainString(), context.getConfiguration());
//        memoryManager.setString(WorldSizeRunner.WORLD_SIZE_OUTPUT_NAME, sum.toPlainString());
//        logger.info("found sum: " + sum.toPlainString());
//        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//        FSDataOutputStream outStream = fileSystem.create(new Path(outputPath));
//        outStream.write(sum.toPlainString().getBytes());         
//        outStream.close();  
    }
}

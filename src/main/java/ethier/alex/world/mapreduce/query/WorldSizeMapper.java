/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.core.data.Element;
import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.core.data.ElementState;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.mapreduce.memory.MemoryManager;
import ethier.alex.world.mapreduce.memory.TaskMemoryManager;
import java.io.*;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldSizeMapper extends Mapper<Text, ElementListWritable, Text, Writable> {

    private static Logger logger = Logger.getLogger(WorldSizeMapper.class);
    private int[] radices;

    @Override
    protected void setup(Context context) throws IOException {
        int mapperId = context.getTaskAttemptID().getTaskID().getId();
        logger.info("Setting up mapper: " + mapperId);


        MemoryManager memoryManager = new TaskMemoryManager(context);
        Map<String, String> memoryMap = memoryManager.syncMemory();

        String serializedRadices = memoryMap.get(WorldSizeRunner.MEMORY_RADICES_NAME);
        radices = Partition.deserializeRadices(serializedRadices);
        logger.info("Setup complete.");
    }

    @Override
    public void map(Text key, ElementListWritable value, Context context) throws IOException, InterruptedException {

        ElementList elementList = value.getElementList();

        BigDecimal weight = BigDecimal.valueOf(1L);
        for (int i = 0; i < elementList.getLength(); i++) {
            Element element = elementList.getElement(i);

            if (element.getElementState() == ElementState.ALL) {
                weight = weight.multiply(BigDecimal.valueOf(radices[i]));
            }
        }

        context.write(key, new BigDecimalWritable(weight));
    }
}

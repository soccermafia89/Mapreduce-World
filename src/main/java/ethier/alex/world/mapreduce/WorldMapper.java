/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.core.processor.SimpleProcessor;
import java.io.IOException;
import java.util.Collection;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldMapper extends Mapper<Text, PartitionWritable, Text, Writable> {

    private static Logger logger = Logger.getLogger(WorldMapper.class);
    
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) {

        int mapperId = context.getTaskAttemptID().getTaskID().getId();
        mos = new MultipleOutputs(context);
        logger.info("Setting up mapper: " + mapperId);
        logger.info("Setup complete.");
    }

    @Override
    public void map(Text key, PartitionWritable value, Context context) {

        Partition partition = value.getPartition();
        SimpleProcessor simpleProcessor = new SimpleProcessor(partition);

        simpleProcessor.runSet();

        Collection<ElementList> elements = simpleProcessor.getCompletedPartitions();
        context.getCounter("Statistics", "Completed Partitions").increment(elements.size());
        
        Collection<Partition> incompletePartitions = simpleProcessor.getIncompletePartitions();
        context.getCounter("Statistics", "Passed Partitions").increment(incompletePartitions.size());

        try {
            
            for (ElementList element : elements) {
                mos.write("seq2", key, new ElementListWritable(element));
//                context.write(key, new ElementListWritable(element));
            }
                        
            for(Partition incompletePartition : incompletePartitions) {
                mos.write("seq1", key, new PartitionWritable(incompletePartition));
//                context.write(key, new PartitionWritable(incompletePartition));
            }

        } catch (IOException e) {
            throw new RuntimeException("Unable to write element.  Caused by: " + ExceptionUtils.getFullStackTrace(e));
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to write element.  Caused by: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
}

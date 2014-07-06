/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.core.processor.SimpleProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldMapper extends Mapper<Text, PartitionWritable, Text, Writable> {

    private static Logger logger = Logger.getLogger(WorldMapper.class);
    
//    private MultipleOutputs mos;
    HdfsOutput outputWriter;
    int initialRunNumber;

    @Override
    protected void setup(Context context) {

        initialRunNumber = Integer.parseInt(context.getConfiguration().get(WorldRunner.RUN_INTITIAL_PARTITIONS_KEY));
        
        int mapperId = context.getTaskAttemptID().getTaskID().getId();
        outputWriter = new HdfsOutput(context);
        logger.info("Setting up mapper: " + mapperId);
        logger.info("Setup complete.");
    }

    @Override
    public void map(Text key, PartitionWritable value, Context context) {

        Partition rootPartition = value.getPartition();
        Collection<Partition> incompletePartitions = new ArrayList<Partition>();
        incompletePartitions.add(rootPartition);
        
        Collection<ElementList> completedPartitions = new ArrayList<ElementList>();
        int processedPartitions = 0;
        while(incompletePartitions.size() < initialRunNumber && incompletePartitions.size() > 0) {
            
            SimpleProcessor simpleProcessor = new SimpleProcessor(incompletePartitions);
            logger.info("Running set over: " + incompletePartitions.size() + " partitions.");
            processedPartitions += incompletePartitions.size();
            
            simpleProcessor.runSet();

            completedPartitions.addAll(simpleProcessor.getCompletedPartitions());
            incompletePartitions = simpleProcessor.getIncompletePartitions();
 
//            logger.info("Finished Set.");
//            logger.info("Completed Partitions: " + completedPartitions.size());
//            logger.info("New Partitions: " + incompletePartitions.size());
//            logger.info("Num Processed Partitions: " + processedPartitions);
//            logger.info("Initial Run Number: " + initialRunNumber);
        }
        
//        if(initialRunNumber < Integer.MAX_VALUE / 20) {
//            initialRunNumber = initialRunNumber*10; 
//        }
        
        context.getCounter("Statistics", "Keys Encountered").increment(1);
        context.getCounter("Statistics", "Processed Partitions").increment(processedPartitions);
        context.getCounter("Statistics", "Completed Partitions").increment(completedPartitions.size());        
        context.getCounter("Statistics", "Passed Partitions").increment(incompletePartitions.size());

        try {
            
            for (ElementList element : completedPartitions) {
                outputWriter.write(WorldRunner.COMPLETE_PARTITION_NAMED_OUTPUT, key, new ElementListWritable(element));
            }
                        
            for(Partition incompletePartition : incompletePartitions) {
                outputWriter.write(WorldRunner.INCOMPLETE_PARTITION_NAMED_OUTPUT, key, new PartitionWritable(incompletePartition));
            }

        } catch (IOException e) {
            throw new RuntimeException("Unable to write element.  Caused by: " + ExceptionUtils.getFullStackTrace(e));
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to write element.  Caused by: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputWriter.close();
    }
}

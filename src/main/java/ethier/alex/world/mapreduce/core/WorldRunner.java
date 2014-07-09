/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.mapreduce.data.PartitionWritable;
import ethier.alex.world.core.data.Partition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**

 @author alex
 */
// Should read in properties 
public class WorldRunner extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(WorldRunner.class);
    public static final String WORK_DIRECTORY_KEY = "ethier.alex.world.mapreduce.work.directory";
    public static final String INCOMPLETE_PARTITION_NAMED_OUTPUT = "incomplete";
    public static final String COMPLETE_PARTITION_NAMED_OUTPUT = "complete";
//    private Path rootPartitionPath;
    private String baseDirectory;
    private Collection<Partition> initialPartitions;
    public static final String RUN_INTITIAL_PARTITIONS_KEY = "ethier.alex.world.mapreduce.intial.partitions";

    public WorldRunner(Partition myPartition, String myBaseDirectory) {
        initialPartitions = new ArrayList<Partition>();
        initialPartitions.add(myPartition);
        baseDirectory = myBaseDirectory;
        
        logger.info("HDFS Base directory set to: " + baseDirectory);   
    }
    
    public WorldRunner(Collection<Partition> myPartitions, String myBaseDirectory) {
        initialPartitions = myPartitions;
        baseDirectory = myBaseDirectory;

        logger.info("HDFS Work directory set to: " + baseDirectory);
    }

//    public void validateProperties(Properties props) {
//        String workDirectory = props.getProperty(WORK_DIRECTORY_KEY);
//        if (workDirectory == null || workDirectory.isEmpty()) {
//            throw new RuntimeException("Missing required property: " + WORK_DIRECTORY_KEY);
//        }
//    }
    
    public void validateConf(Configuration conf) {
        String initialPartitionRun = conf.get(RUN_INTITIAL_PARTITIONS_KEY);
        try {
            Integer.parseInt(initialPartitionRun);
        } catch (Exception e) {
            throw new RuntimeException("Could not parse required int property in Configuration: " + RUN_INTITIAL_PARTITIONS_KEY + ""
                    + " Caused by: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        
        this.validateConf(getConf());

        this.writeRootPartition();

        Path inputPath = new Path(baseDirectory + "/root");
        int runCounter = 0;
        JobConf jobConf = null;

        while (jobConf == null || hasIncompletePartitions(jobConf, inputPath)) {

            jobConf = new JobConf(getConf());
            logger.info("Setting mapper memory to 1G");
            jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx1g");

            logger.info("Setting max number of attempts to 1.");
            jobConf.setMaxMapAttempts(1);
            jobConf.setJarByClass(this.getClass());


            Job job = new Job(jobConf);

            job.setJobName(this.getClass().getName());
            job.setMapperClass(WorldMapper.class);
            job.setNumReduceTasks(0);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(job, inputPath);
            logger.info("Reading input at [" + SequenceFileInputFormat.getInputPaths(job)[0].toString() + "]");

            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
            HdfsOutput.setupDefaultOutput(job, new Path(baseDirectory + "/default"));
            HdfsOutput.addNamedOutput(job, INCOMPLETE_PARTITION_NAMED_OUTPUT, baseDirectory + "/outputIncomplete", SequenceFileOutputFormat.class, Text.class, PartitionWritable.class);
            HdfsOutput.addNamedOutput(job, COMPLETE_PARTITION_NAMED_OUTPUT, baseDirectory + "/completed", SequenceFileOutputFormat.class, Text.class, ElementListWritable.class);

            int success = job.waitForCompletion(true) ? 0 : 1;

            if (success == 1) {
                logger.error("Unsuccessful job run for run counter: " + runCounter);
                return 1;
            }

            if (runCounter > 2) {
                logger.error("Breaking after max runs.");
                break;
            }

//            String incompletePartitionsPath = HdfsOutput.getNamedOutputPath(job, INCOMPLETE_PARTITION_NAMED_OUTPUT);
            inputPath = new Path(baseDirectory + "/inputIncomplete");
            FileSystem fileSystem = FileSystem.get(jobConf);
            fileSystem.delete(inputPath, true);
            logger.info("Moving incomplete partitions back to input.");
            fileSystem.rename(new Path(baseDirectory + "/outputIncomplete"), inputPath);

//            HdfsOutput.clearNamedOutputs(jobConf);

            runCounter++;
        }
        
        return 0;
    }

    public boolean hasIncompletePartitions(Configuration conf, Path inputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        
        return fileSystem.exists(inputPath);
    }

    public void writeRootPartition() throws IOException {
        
        Path rootPartitionPath = new Path(baseDirectory + "/root");
        logger.info("Writing root partition to [" + rootPartitionPath.toString() + "]");

        JobConf jobRootConf = new JobConf();
        FileSystem fileSystem = FileSystem.get(jobRootConf);
        fileSystem.delete(rootPartitionPath, true);

        SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(rootPartitionPath);
        SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(PartitionWritable.class);
        SequenceFile.Writer writer = SequenceFile.createWriter(jobRootConf, optPath, optKey, optVal);
        for(Partition initialPartition : initialPartitions) {
                writer.append(new Text("Partition"), new PartitionWritable(initialPartition));
        }
        writer.close();
    }
}

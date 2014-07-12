/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.mapreduce.memory.HdfsMemoryManager;
import ethier.alex.world.mapreduce.memory.MemoryJob;
import ethier.alex.world.mapreduce.memory.MemoryToken;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class ResultExportRunner extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(ResultExportRunner.class);
    
//    public static final String RESULT_NAMED_OUTPUT = "results";
    public static final String RESULT_NAMED_OUTPUT = "ethier.alex.world.mapreduce.result.output";
    
    private Path elementListPath;
    private String tmpDirectory;
    private String outputPath;
    
    private Collection<ElementList> completedPartitions;

    public ResultExportRunner(Path myElementListPath, String myOutputPath, String myTmpDirectory) {
        elementListPath = myElementListPath;
        tmpDirectory = myTmpDirectory;
        outputPath = myOutputPath;
    }

    @Override
    public int run(String[] strings) throws Exception {

        JobConf jobConf = new JobConf(getConf());
        logger.info("Setting reducer memory to 1G");
        jobConf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, "-Xmx1g");

        logger.info("Setting max number of attempts to 1.");
        jobConf.setJarByClass(this.getClass());
        jobConf.setMaxReduceAttempts(1);
        
//        jobConf.set(RESULT_OUTPUT_KEY, outputPath);


        MemoryJob job = new MemoryJob(jobConf);

        job.setJobName(this.getClass().getName());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ElementListWritable.class);
        
        job.setReducerClass(ResultReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, elementListPath);
        logger.info("Reading input at [" + elementListPath.toString() + "]");

        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(tmpDirectory));
        
        FileSystem fileSystem = FileSystem.get(jobConf);
        logger.error("TODO CHECK: why delete/need output path: outputPath => " + outputPath);
        fileSystem.delete(new Path(outputPath), true);
        fileSystem.delete(new Path(tmpDirectory), true);
//        HdfsOutput.setupDefaultOutput(job, new JobC(workDirectory + "/defaultOutput"));
//        HdfsOutput.addNamedOutput(job, RESULT_NAMED_OUTPUT, workDirectory + "/results", FileOutputFormat.class, Text.class, Text.class);

//        HdfsOutput.clearNamedOutputs(jobConf);
        MemoryToken memoryToken = job.openConnection();
        int ret = job.waitForCompletion(true) ? 0 : 1;
        String resultStr = HdfsMemoryManager.getString(RESULT_NAMED_OUTPUT, job.getConfiguration());
        memoryToken.close();
        Collection<byte[]> bytes = CollectionByteSerializer.toBytes(resultStr);
//                logger.info("Collection size: " + bytes.size());
        completedPartitions = new ArrayList<ElementList>();
        for(byte[] byteArray : bytes) {
            ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
            DataInput dataInput = new DataInputStream(bais);
            ElementList elementList = new ElementList(dataInput);
//            logger.info("Retreived element list: " + elementList.toString());
            completedPartitions.add(elementList);
        }
        
        return ret;
    }
    
    public Collection<ElementList> getCompletedPartitions() {
        return completedPartitions;
    }
}

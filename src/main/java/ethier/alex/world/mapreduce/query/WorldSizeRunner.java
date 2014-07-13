/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.memory.HdfsMemoryManager;
import ethier.alex.world.mapreduce.memory.MemoryJob;
import ethier.alex.world.mapreduce.memory.MemoryToken;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class WorldSizeRunner extends Configured implements Tool  {
    
    private static Logger logger = Logger.getLogger(WorldSizeRunner.class);
    
    public static final String WORLD_SIZE_OUTPUT_NAME = "worldSize";
    
//    private String outputDirectory;
    private String inputPath;
    private String tmpDirectory;
    private int[] radices;
    private String worldSize;
//    JobConf conf;
    
    public WorldSizeRunner(String myIputPath, String myTmpDirectory, int[] myRadices) {
        inputPath = myIputPath;
        tmpDirectory = myTmpDirectory;
//        outputDirectory = myOutputDirectory;
        logger.info("Reading input at: [" + inputPath + "]");
        radices = myRadices;
    }

    @Override
    public int run(String[] strings) throws Exception {
            logger.info("Setting mapper memory to 1G");
            JobConf conf = new JobConf(getConf());
            
            conf.set(QueryRunner.RADICES_KEY, QueryRunner.serializeRadices(radices));
//            conf.set(WorldSizeRunner.WORLD_SIZE_OUTPUT_KEY, outputDirectory);
            logger.info("CONFIGURATION SETTINGS HARD CODED, FIX LATER.");
            conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx1g");
            conf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, "-Xmx1g");

            logger.info("Setting max number of attempts to 1.");
            conf.setMaxMapAttempts(1);
            conf.setJarByClass(this.getClass());


            MemoryJob job = new MemoryJob(conf);

            job.setJobName(this.getClass().getName());
            job.setMapperClass(WorldSizeMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BigDecimalWritable.class);
            job.setReducerClass(WorldSizeReducer.class);
            
            job.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(job, inputPath);
            
            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(tmpDirectory));
            
            FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.delete(new Path(tmpDirectory), true);
            MemoryToken memoryToken = job.openConnection();
//            HdfsMemoryManager manager = HdfsMemoryManager.openManager(job.getConfiguration());
            
            int ret = job.waitForCompletion(true) ? 0 : 1;
            
            logger.info("Completed World Size Job, closing manager, ret: " + ret);
            if(ret == 0) {
                worldSize = HdfsMemoryManager.getString(WorldSizeRunner.WORLD_SIZE_OUTPUT_NAME, conf);
            }
            memoryToken.close();
            
            return ret;
    }
    
    public String getWorldSize() {
        return worldSize;
    }
}

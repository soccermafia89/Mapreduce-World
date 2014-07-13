/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.mapreduce.memory.HdfsMemoryManager;
import ethier.alex.world.mapreduce.memory.MemoryJob;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.memory.MemoryToken;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
public class QueryRunner extends Configured implements Tool {
    
    private static Logger logger = Logger.getLogger(QueryRunner.class);
    
//    public static final String FILTER_INPUT_PATH_KEY = "ethier.alex.world.mapreduce.query.filter.input";
//    public static final String RADICES_KEY = "ethier.alex.world.mapreduce.query.radices";
//    public static final String WORLD_SIZE_KEY = "ethier.alex.world.mapreduce.query.world.size";
    public static final String MEMORY_QUERY_NAME = "queryOutput";
    public static final String MEMORY_FILTERS_NAME = "filtersInput";
    public static final String MEMORY_RADICES_NAME = "radicesInput";
    
    private Collection<FilterList> filters;
    private String elementListPath;
    private String baseDirectory;
    int[] radices;
    String worldSize;
    private String probabilityOutput;
//    HdfsMemoryManager memoryManager;
    
    public QueryRunner(String myWorldSize, FilterList filter, String myElementListPath, int[] myRadices, String myBaseDirectory) {
        filters = new ArrayList<FilterList>();
        filters.add(filter);
        elementListPath = myElementListPath;
        baseDirectory = myBaseDirectory;
        radices = myRadices;
        worldSize = myWorldSize;
    }
    
    public QueryRunner(String myWorldSize, Collection<FilterList> myFilters, String myElementListPath, int[] myRadices, String myBaseDirectory) {
        filters = myFilters;
        elementListPath = myElementListPath;
        baseDirectory = myBaseDirectory;
        radices = myRadices;
        worldSize = myWorldSize;
    }

    @Override
    public int run(String[] strings) throws Exception {
//        this.writeFilters();
        
        Configuration conf = getConf();
//        conf.set(RADICES_KEY, QueryRunner.serializeRadices(radices));
//        conf.set(WORLD_SIZE_KEY, worldSize);
//        conf.set(FILTER_INPUT_PATH_KEY, baseDirectory + "/filters");
        
        
        JobConf jobConf = new JobConf(getConf());
//        logger.info("Setting mapper memory to 1G");
//        jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx1g");
//
//        logger.info("Setting max number of attempts to 1.");
//        jobConf.setMaxMapAttempts(1);
        jobConf.setJarByClass(this.getClass());
//
//
//        jobConf.
        MemoryJob job = new MemoryJob(jobConf);

        job.setJobName(this.getClass().getName());
        job.setMapperClass(QueryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BigDecimalWritable.class);
        job.setReducerClass(QueryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BigDecimalWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, new Path(elementListPath));
        logger.info("Reading input at [" + elementListPath.toString() + "]");

        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(baseDirectory + "/query"));
        
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(baseDirectory + "/query"), true);
        
        MemoryToken memoryToken = job.openConnection();
        String serializedFilters = FilterList.serializeFilters(filters);
        String serializedRadices = Partition.serializeRadices(radices);
        HdfsMemoryManager.setString(QueryRunner.MEMORY_FILTERS_NAME, serializedFilters, job.getConfiguration());
        HdfsMemoryManager.setString(QueryRunner.MEMORY_RADICES_NAME, serializedRadices, job.getConfiguration());
        HdfsMemoryManager.setString(WorldSizeRunner.MEMORY_WORLD_SIZE_NAME, worldSize, job.getConfiguration());
        int ret = job.waitForCompletion(true) ? 0 : 1;
        probabilityOutput = HdfsMemoryManager.getString(QueryRunner.MEMORY_QUERY_NAME, job.getConfiguration());
        memoryToken.close();
        return ret;
    }
    
    public String getProbabilityOutput() {
        return probabilityOutput;
    }
}

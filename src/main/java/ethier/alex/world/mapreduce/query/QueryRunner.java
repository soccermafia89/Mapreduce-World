/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
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
public class QueryRunner extends Configured implements Tool {
    
    private static Logger logger = Logger.getLogger(QueryRunner.class);
    
    public static final String FILTER_INPUT_PATH_KEY = "ethier.alex.world.mapreduce.query.filter.input";
    public static final String RADICES_KEY = "ethier.alex.world.mapreduce.query.radices";
    public static final String WORLD_SIZE_KEY = "ethier.alex.world.mapreduce.query.world.size";
    
    private Collection<FilterList> filters;
    private String elementListPath;
    private String baseDirectory;
    int[] radices;
    String worldSize;
    
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
        this.writeFilters();
        
        Configuration conf = getConf();
        conf.set(RADICES_KEY, QueryRunner.serializeRadices(radices));
        conf.set(WORLD_SIZE_KEY, worldSize);
        conf.set(FILTER_INPUT_PATH_KEY, baseDirectory + "/filters");
        
        JobConf jobConf = new JobConf(getConf());
//        logger.info("Setting mapper memory to 1G");
//        jobConf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, "-Xmx1g");
//
//        logger.info("Setting max number of attempts to 1.");
//        jobConf.setMaxMapAttempts(1);
        jobConf.setJarByClass(this.getClass());
//
//
        Job job = new Job(jobConf);

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
//        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    // This method does not belong here.
    public void writeFilters() throws IOException {

        Collection<byte[]> byteCollection = new ArrayList<byte[]>();
        for(FilterList filter : filters) {
            logger.info("Writing filter: " + filter);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            filter.write(dataOutput);
            byte[] serializedElementList = baos.toByteArray();
            byteCollection.add(serializedElementList);
        }

        FileSystem fileSystem = FileSystem.get(getConf());
        FSDataOutputStream outStream = fileSystem.create(new Path(baseDirectory + "/filters"));
        logger.info("Filters written to: " + baseDirectory + "/filters");
        String serializedResults = CollectionByteSerializer.toString(byteCollection);
        outStream.write(serializedResults.getBytes());         
        outStream.close();
    }
    
    public static String serializeRadices(int[] radices) {
        
        StringBuilder stringBuilder = new StringBuilder();
        
        for(int i=0; i < radices.length; i++) {
            int radix = radices[i];
            stringBuilder.append(radix);
            stringBuilder.append(",");
        }
        
        return StringUtils.stripEnd(stringBuilder.toString(), ",");
    }
    
    public static int[] deserializeRadices(String serializedRadices) {
        String[] intStrs = serializedRadices.split(",");
        int[] radices = new int[intStrs.length];
        for(int i=0; i < intStrs.length; i++) {
            radices[i] = Integer.parseInt(intStrs[i]);
        }
        
        return radices;
    }
}

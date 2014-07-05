/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class HdfsOutput {
    
    private static Logger logger = Logger.getLogger(WorldMapper.class);
    
    private static final String ROOT_PATH_KEY = "ethier.alex.world.mapreduce.root.path";
    
//    private static Map<JobID, String> jobIds;
//    private static Set<String> namedOutputs =  new HashSet<String>();
    private MultipleOutputs mos;
    private Context context;
    
    public HdfsOutput(Context myContext) {
        
        mos = new MultipleOutputs(myContext);
        context = myContext;
        
        logger.info("File Manager Setup Successfully!");
    }
    
    public static Path getRootPath(Job job) {
        
        String rootPathStr = job.getConfiguration().get(ROOT_PATH_KEY);
        return new Path(rootPathStr);
    }
    
    public static Path getRootPath(Context context) {
        
        String rootPathStr = context.getConfiguration().get(ROOT_PATH_KEY);
        return new Path(rootPathStr);
    }
    
    public static Path getRootPath(Configuration conf) {
        
        String rootPathStr = conf.get(ROOT_PATH_KEY);
        return new Path(rootPathStr);
    }
    
    public static void setRootPath(Job job, Path myRootPath) throws IOException {
//        rootPath = myRootPath;
        if(job.getConfiguration().get(ROOT_PATH_KEY) != null && !job.getConfiguration().get(ROOT_PATH_KEY).isEmpty()) {
            logger.warn("Overriding property: " + ROOT_PATH_KEY + ", was " + job.getConfiguration().get(ROOT_PATH_KEY));
        }
        job.getConfiguration().set(ROOT_PATH_KEY, myRootPath.toString());
        
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        Path defaultOutputPath = new Path(myRootPath.toString() + "/defaultOutput");
        fileSystem.delete(defaultOutputPath, true);
        SequenceFileOutputFormat.setOutputPath(job, defaultOutputPath);
    }
    
//    public static void moveDefaultOutput(Configuration conf, Path newPath) throws IOException {
//        FileSystem fileSystem = FileSystem.get(conf);
//        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path(HdfsOutput.getRootPath(conf).toString() + "/defaultOutput"), true);
//        while(it.hasNext()) {
//            Path filePath = it.next().getPath();
//            fileSystem.rename(filePath, newPath);
//        }   
//    }
    
    public static void clearNamedOutputs(Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(HdfsOutput.getRootPath(conf).toString() + "/output"), true);
    }
    
    public static void addNamedOutput(Job job, String namedOutput, Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass) {
//        if(jobIds == null) {
//            jobIds = new HashMap<JobID, String>();
//        }
//        
//        JobID jobID = job.getJobID();
//        jobIds.put(jobID, namedOutput);
//        String key = job.getJobID().
        
//        if(namedOutputs.contains(namedOutput)) {
//            logger.warn("Named output already exists: " + namedOutput);
//            return;
//        } else {
//            namedOutputs.add(namedOutput);
//        }
        
        MultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, keyClass, valueClass);        
    }
    
    public <K extends Object, V extends Object> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
        
        String baseOutputPath = HdfsOutput.getRootPath(context).toString() + "/output/" + namedOutput + "/" + namedOutput;
        mos.write(namedOutput, key, value, baseOutputPath);
    }
    
    public static Path getNamedOutput(Job job, String namedOutput) {
        return new Path(HdfsOutput.getRootPath(job).toString() + "/output/" + namedOutput);
    }
    
    public void close() throws IOException, InterruptedException {
        mos.close();
    }
}

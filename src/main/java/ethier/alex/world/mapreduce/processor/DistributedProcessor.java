/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.processor;

import ethier.alex.world.core.data.ElementList;
import ethier.alex.world.core.data.Partition;
import ethier.alex.world.core.processor.Processor;
import ethier.alex.world.mapreduce.core.ResultExportRunner;
import ethier.alex.world.mapreduce.core.WorldRunner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class DistributedProcessor implements Processor {
    
    private static Logger logger = Logger.getLogger(DistributedProcessor.class);
    
    private String baseDirectory;
    private Collection<Partition> initialPartitions;
    private Configuration conf;
    private String[] args;
    
    private boolean hasRun = false;
    
    public DistributedProcessor(Partition myPartition, String myBaseDirectory, Properties props, String[] myArgs) {
        baseDirectory = myBaseDirectory;
        initialPartitions = new ArrayList<Partition>();
        initialPartitions.add(myPartition);
        conf = this.transformProperties(props);
        args = myArgs;
    }
    
    public DistributedProcessor(Collection<Partition> myPartitions, String myBaseDirectory, Properties props, String[] myArgs) {
        baseDirectory = myBaseDirectory;
        initialPartitions = myPartitions;
        conf = this.transformProperties(props);
        args = myArgs;
    }
    
    public DistributedProcessor(Partition myPartition, String myBaseDirectory, Configuration myConf, String[] myArgs) {
        baseDirectory = myBaseDirectory;
        initialPartitions = new ArrayList<Partition>();
        initialPartitions.add(myPartition);
        conf = myConf;
        args = myArgs;
    }
    
    public DistributedProcessor(Collection<Partition> myPartitions, String myBaseDirectory, Configuration myConf, String[] myArgs) {
        baseDirectory = myBaseDirectory;
        initialPartitions = myPartitions;
        conf = myConf;
        args = myArgs;
    }

    @Override
    public Collection<ElementList> getCompletedPartitions() {
        
        if(!hasRun) {
            return new ArrayList<ElementList>();
        } else {
            ResultExportRunner resultExportRunner = new ResultExportRunner(new Path(baseDirectory + "/completed"), baseDirectory + "/default");
            
            int ret = -1;
            try {
                ret = ToolRunner.run(conf, resultExportRunner, args);

                if(ret != 0) {
                    throw new RuntimeException("Tool Runner failed.");
                } else {
                    return resultExportRunner.getCompletedPartitions();
                }
            } catch (Exception e) {
                throw new RuntimeException("Unable to export parititons.  Caused by: " + ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    @Override
    public Collection<Partition> getIncompletePartitions() {
        if(!hasRun) {
            return initialPartitions;
        } else {
            return new ArrayList<Partition>();
        }
    }

    @Override
    public void runAll() {
        try {
            WorldRunner worldRunner = new WorldRunner(initialPartitions, baseDirectory);
            int ret = ToolRunner.run(conf, worldRunner, args);//args must be passed in from shell.
            if(ret != 0) {
                throw new RuntimeException("Mapreduce job failed to process partitions.");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Could not process partitions.  Caused by: " + ExceptionUtils.getFullStackTrace(ex));
        }
        
        hasRun = true;
    }

    @Override
    public void runSet() {
        this.runAll();
    }
    
    private Configuration transformProperties(Properties props) {
        Configuration conf = new Configuration();
        for(Object key : props.keySet()) {
            if(key instanceof String) {
                String keyStr = (String) key;
                String valStr = props.getProperty(keyStr);
                
                conf.set(keyStr, valStr);
            }
        }
        return conf;
    }
}

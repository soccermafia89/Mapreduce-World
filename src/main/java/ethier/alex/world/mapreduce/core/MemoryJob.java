/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**

 @author alex
 */
public class MemoryJob extends Job {
    
    MemoryToken memoryToken;

//    public MemoryJob() throws IOException {
//        throw 
//        super();
//    }

    public MemoryJob(Configuration conf) throws IOException {
        super(conf);
    }

    public MemoryJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
    }
    
    public MemoryToken getMemoryToken() {
        return memoryToken;
    }
}

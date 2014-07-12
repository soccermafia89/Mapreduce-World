/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.memory;

import ethier.alex.world.mapreduce.memory.MemoryToken;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**

 @author alex
 */


// TODO : This class could use major clean up.

/*

HdfsMemoryManager should lazily setup the connection (create the conf-id folder).

this means it will need a constructor to store setup information and only when a new method,
open connection2 is called does it create the connection, this method is only called when the job
calls open connection.

then remove the first token from the wait for completion method.

*/
public class MemoryJob extends Job {

//    MemoryToken memoryToken;
    private static HdfsMemoryManager manager = new HdfsMemoryManager();
    private MemoryToken memoryToken;
    private Configuration conf;

    public MemoryJob(Configuration myConf) throws IOException {
        super(manager.openConnection(myConf));
        memoryToken = manager.getMemoryToken();
        conf = myConf;
    }

    public MemoryJob(Configuration myConf, String jobName) throws IOException {
        super(manager.openConnection(myConf), jobName);
        memoryToken = manager.getMemoryToken();
        conf = myConf;
    }

    @Override
    protected void finalize() throws Throwable {
        memoryToken.close();

        super.finalize();
    }
    
    public MemoryToken openConnection() throws IOException {
        HdfsMemoryManager hdfsMemoryManager = new HdfsMemoryManager();
        hdfsMemoryManager.openConnection(conf);
        return hdfsMemoryManager.getMemoryToken();
    }

    @Override
    public boolean waitForCompletion(boolean bool) throws IOException, InterruptedException, ClassNotFoundException {
        boolean toReturn;
        try {
            toReturn = super.waitForCompletion(bool);
        } finally {
            memoryToken.close();
        }
        
        return toReturn;
    }
}

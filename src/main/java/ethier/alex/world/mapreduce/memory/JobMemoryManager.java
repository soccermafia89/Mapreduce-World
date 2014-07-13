/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.memory;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class JobMemoryManager extends MemoryManager {
    
    private static Logger logger = Logger.getLogger(JobMemoryManager.class);

    public JobMemoryManager() {
    }

    public Configuration writeConfiguration(Configuration myConf) throws IOException {

        String currentConfId = myConf.get(MEMORY_MANAGER_CONF_ID_KEY);
        if (currentConfId != null && !currentConfId.isEmpty()) {
            throw new RuntimeException("Configuration already set, only setup one HdfsMemoryManager per job.");
        }

        super.confId = "conf-id-" + Math.random();
        myConf.set(MEMORY_MANAGER_CONF_ID_KEY, super.confId);


        super.conf = myConf;

        return super.conf;
    }

    public void openConnection() throws IOException {

        // Require job input to prevent connections from being opened within tasks.
        FileSystem fileSystem = FileSystem.get(super.conf);
        fileSystem.mkdirs(new Path("/MemoryManager/" + super.confId));
    }

    public Map<String, String> closeConnection() throws IOException {

        // Require job input to prevent connections from being opened within tasks.
        FileSystem fileSystem = FileSystem.get(super.conf);
        fileSystem.delete(new Path("/MemoryManager/" + super.confId), true);
        return super.cleanMemory();
    }
}

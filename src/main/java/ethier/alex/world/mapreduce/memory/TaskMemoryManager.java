/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.memory;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class TaskMemoryManager extends MemoryManager {

    private static Logger logger = Logger.getLogger(TaskMemoryManager.class);

    public TaskMemoryManager(TaskAttemptContext context) {
        super.conf = context.getConfiguration();
        super.confId = super.conf.get(MEMORY_MANAGER_CONF_ID_KEY);
        if (super.confId == null || super.confId.isEmpty()) {
            throw new RuntimeException("Configuration not set, use MemoryJob to set properly.");
        }
    }

    public void setString(String key, String value) throws IOException {
        if (!StringUtils.isAlphanumeric(key)) {
            throw new RuntimeException("Memory map keys must be alphanumeric.");
        }

        Path path = new Path("/MemoryManager/" + super.confId + "/" + key);
        FileSystem fileSystem = FileSystem.get(super.conf);
        
        if(fileSystem.isFile(path)) {
            throw new RuntimeException("Key already exists: " + key);
        } else {
            FSDataOutputStream out = fileSystem.create(path);
            out.write(value.getBytes());
            out.close();
        }
    }
}

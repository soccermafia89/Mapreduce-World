/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**

 @author alex
 */
// Acts as a datastore to pull simple values in/out of jobs.
// All written values are treated as final.
// There is a new datastore uri for each Configuration, so storage can only be accessed by a per job basis.
// The memory location will always be cleaned up by the end of the jvm execution.
// To prevent premature clean up, openConnection and setString will return a MemoryToken object.
// As long as the MemoryToken object is held in memory, clean up will not occur.

// NOTE: Due to the way hadoop's garbage collector works, a connection should never be opened within a task.
// This is why the openConnection requires a Job input instead of a Configuration input.
public class HdfsMemoryManager {

    private static Logger logger = Logger.getLogger(HdfsMemoryManager.class);
    public static final String MEMORY_MANAGER_CONF_ID_KEY = "ethier.alex.world.mapreduce.core.memory.manager.id";

    public static MemoryToken openConnection(Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        String presetConfId = conf.get(MEMORY_MANAGER_CONF_ID_KEY);
        if (presetConfId == null || presetConfId.isEmpty()) {            
            String confId = "conf-id-" + Math.random();
            logger.info("Creating new connection to: " + "/MemoryManager/" + confId);
            conf.set(MEMORY_MANAGER_CONF_ID_KEY, confId);

            if (fileSystem.isDirectory(new Path("MemoryManager/" + confId))) {
                throw new RuntimeException("Random conf-id path already exists at: " + "/MemoryManager/" + confId);
            }
            fileSystem.mkdirs(new Path("/MemoryManager/" + confId));
        }

        return new MemoryToken(conf);
    }

    //Main methods
    public static void setString(String name, String output, Configuration conf) throws IOException {
        

        Path writePath = HdfsMemoryManager.getMemoryPath(name, conf);
        
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.isFile(writePath)) {
            throw new RuntimeException("Named memory location already set: " + writePath);
        }
        
        logger.info("Writing to : " + writePath);
        FSDataOutputStream outStream = fileSystem.create(writePath);
        outStream.write(output.getBytes());
        outStream.close();
    }

    public static String getString(String name, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path readPath = HdfsMemoryManager.getMemoryPath(name, conf);
        FSDataInputStream inputStream = fileSystem.open(readPath);
        byte[] bytes = IOUtils.toByteArray(inputStream);
        inputStream.close();
        return new String(bytes);
    }
    
    private static Path getMemoryPath(String name, Configuration conf) {
        String confId = conf.get(MEMORY_MANAGER_CONF_ID_KEY);
        if(confId == null || confId.isEmpty()) {
            throw new RuntimeException("Connection never opened for Configuraton object.  "
                    + "Call HdfsMemoryManager.openConnection first, before Job creation.");
        }
        
        return new Path("/MemoryManager/" + confId + "/names/" + name);
    }
}

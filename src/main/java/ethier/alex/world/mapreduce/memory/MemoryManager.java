/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.memory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

/**

 @author alex
 */

/**
TODO: Fix the memory manager implementation.

Instead of having two classes extend a base memory manager, completely separate them into two different classes.
It will help reduce the clutter.
*/
public class MemoryManager {

    private static Logger logger = Logger.getLogger(MemoryManager.class);
    public static final String MEMORY_MANAGER_CONF_ID_KEY = "ethier.alex.world.mapreduce.core.memory.manager.id";
    protected Configuration conf;
    protected String confId;
    private Map<String, String> memoryMap = new HashMap<String, String>();

    public void addToMemory(String key, String value) {
        if (!StringUtils.isAlphanumeric(key)) {
            throw new RuntimeException("Memory map keys must be alphanumeric.");
        }

        if (memoryMap.containsKey(key)) {
            throw new RuntimeException("Immutable memory map key: " + key + " already set.");
        } else {
            memoryMap.put(key, value);
        }
    }

    public String getFromMemory(String key) {
        if (memoryMap.containsKey(key)) {
            return memoryMap.get(key);
        } else {
            throw new RuntimeException("Memory map is missing key: " + key);
        }
    }

    public Map<String, String> syncMemory() throws IOException {
        logger.info("Syncing memory.");
        FileSystem fileSystem = FileSystem.get(conf);

        if (!fileSystem.isDirectory(new Path("/MemoryManager/" + confId))) {
            throw new RuntimeException("Cannot sync memory manager, connection not open.");
        } else {
            RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("/MemoryManager/" + confId), true);
            Map<String, Path> hdfsMap = new HashMap<String, Path>();
            while (it.hasNext()) {
                Path path = it.next().getPath();
                hdfsMap.put(path.getName(), path);
            }

            Set<String> localKeys = memoryMap.keySet();
            Set<String> hdfsKeys = hdfsMap.keySet();

            for (String key : localKeys) {
                if (!hdfsKeys.contains(key)) {
                    logger.info("Updating hdfs memory map for key: " + key);

                    // Write to hdfs
                    String value = memoryMap.get(key);
                    FSDataOutputStream out = fileSystem.create(new Path("/MemoryManager/" + confId + "/" + key));
                    out.write(value.getBytes());
                    out.close();
                }
            }

            for (String key : hdfsKeys) {
                if (!localKeys.contains(key)) {
                    // Read from hdfs
                    Path path = hdfsMap.get(key);
                    FSDataInputStream in = fileSystem.open(path);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    IOUtils.copy(in, baos);
                    memoryMap.put(key, new String(baos.toByteArray()));
                }
            }
        }

        return memoryMap;
    }
    
    public Map<String, String> cleanMemory() {
        Map<String, String> returnMap = new HashMap<String,String>(memoryMap);
        memoryMap.clear();
        return returnMap;
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**

 @author alex
 */
public class MemoryJob extends Job {

    private static JobMemoryManager manager = new JobMemoryManager();
    private Map<String, String> archiveMemoryMap = new HashMap<String, String>();

    public MemoryJob(Configuration myConf) throws IOException {
        super(manager.writeConfiguration(myConf));
    }

    public MemoryJob(Configuration myConf, String jobName) throws IOException {
        super(manager.writeConfiguration(myConf), jobName);
//        conf = myConf;
    }

    public void addToMemory(String key, String value) {
        manager.addToMemory(key, value);
    }

    public String getFromMemory(String key) {
        if (archiveMemoryMap.containsKey(key)) {
            return archiveMemoryMap.get(key);
        } else {
            throw new RuntimeException("Missing key in memory map: " + key);
        }
    }

    @Override
    public boolean waitForCompletion(boolean bool) throws IOException, InterruptedException, ClassNotFoundException {
        boolean toReturn;
        try {
            manager.openConnection();
            manager.syncMemory();
            toReturn = super.waitForCompletion(bool);
            manager.syncMemory();
        } finally {
            archiveMemoryMap = manager.closeConnection();
        }

        return toReturn;
    }
}

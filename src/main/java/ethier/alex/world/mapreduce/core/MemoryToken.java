/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class MemoryToken {

    private static Logger logger = Logger.getLogger(MemoryToken.class);
    private String confId;
    private String tokenId;
    private FileSystem fileSystem;

    public MemoryToken(Configuration conf) throws IOException {
        confId = conf.get(HdfsMemoryManager.MEMORY_MANAGER_CONF_ID_KEY);
        fileSystem = FileSystem.get(conf);
        tokenId = "token-id-" + Math.random();

        if (!fileSystem.isDirectory(new Path("/MemoryManager/" + confId))) {
            throw new RuntimeException("Cannot create memory token, memory connection closed at: " + "/MemoryManager/" + confId);
        }
        
        Path tokenPath = new Path("/MemoryManager/" + confId + "/tokens/" + tokenId);

        if(fileSystem.isFile(tokenPath)) {
            throw new RuntimeException("Random token file already exists at: " + "/MemoryManager/" + confId + "/tokens/" + tokenId);
        }
        
        logger.info("Memory token registered at: " + tokenPath);
        
        FSDataOutputStream outStream = fileSystem.create(tokenPath);
        outStream.writeInt(1);
        outStream.close();
    }

    public void close() throws IOException {
        this.cleanFileSystem();
    }

    @Override
    protected void finalize() throws Throwable {
        this.cleanFileSystem();

        super.finalize();
    }

    private void cleanFileSystem() throws IOException {
        logger.info("Closing Memory Token at: " + "/MemoryManager/" + confId + "/tokens/" + tokenId);
        fileSystem.delete(new Path("/MemoryManager/" + confId + "/tokens/" + tokenId), true);
        
        //TMP DEBUG
//        RemoteIterator<LocatedFileStatus> itTmp = fileSystem.listFiles(new Path(confIdPath + "/tokens/"), true);
//        while(itTmp.hasNext()) {
//            Path path = itTmp.next().getPath();
//            logger.info("")
//        }

        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(new Path("/MemoryManager/" + confId + "/tokens/"), true);
        if (!it.hasNext()) {
            logger.info("Closing memory connection to: " + "/MemoryManager/" + confId);
            fileSystem.delete(new Path("/MemoryManager/" + confId), true);
        }
    }
}

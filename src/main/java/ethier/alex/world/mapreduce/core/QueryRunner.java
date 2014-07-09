/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.core;

import ethier.alex.world.addon.CollectionByteSerializer;
import ethier.alex.world.core.data.FilterList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryRunner extends Configured implements Tool {
    
    private static Logger logger = Logger.getLogger(QueryRunner.class);
    
    private Collection<FilterList> filters;
    private String elementListPath;
    private String workDirectory;
    private Path filterWritePath;
    
    public QueryRunner(Collection<FilterList> myFilters, String myElementListPath, String myWorkingDirectory) {
        filters = myFilters;
        elementListPath = myElementListPath;
        workDirectory = myWorkingDirectory;
        filterWritePath = new Path(workDirectory + "/filters");
    }

    @Override
    public int run(String[] strings) throws Exception {
        this.writeFilters();
        
        return 0;
    }
    
    public void writeFilters() throws IOException {

        Collection<byte[]> byteCollection = new ArrayList<byte[]>();
        for(FilterList filter : filters) {
            logger.info("Writing filter: " + filter);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(baos);
            filter.write(dataOutput);
            byte[] serializedElementList = baos.toByteArray();
            byteCollection.add(serializedElementList);
        }

        FileSystem fileSystem = FileSystem.get(getConf());
        FSDataOutputStream outStream = fileSystem.create(filterWritePath);
        String serializedResults = CollectionByteSerializer.toString(byteCollection);
        outStream.write(serializedResults.getBytes());         
        outStream.close();
    }
}

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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryRunner extends Configured implements Tool {
    
    private static Logger logger = Logger.getLogger(QueryRunner.class);
    
    public static final String FILTER_INPUT_PATH_KEY = "ethier.alex.world.mapreduce.core.filter.input";
    public static final String RADICES_KEY = "ethier.alex.world.mapreduce.core.radices";
    
    private Collection<FilterList> filters;
    private String elementListPath;
    private String baseDirectory;
    private Configuration conf;
//    private Path filterWritePath;
    
    public QueryRunner(Collection<FilterList> myFilters, String myElementListPath, int[] radices, String myBaseDirectory, Configuration myConf) {
        filters = myFilters;
        elementListPath = myElementListPath;
        baseDirectory = myBaseDirectory;
        
        conf.set(RADICES_KEY, QueryRunner.serializeRadices(radices));
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
        FSDataOutputStream outStream = fileSystem.create(new Path(baseDirectory + "/filters"));
        String serializedResults = CollectionByteSerializer.toString(byteCollection);
        outStream.write(serializedResults.getBytes());         
        outStream.close();
    }
    
    public static String serializeRadices(int[] radices) {
        
        StringBuilder stringBuilder = new StringBuilder();
        
        for(int i=0; i < radices.length; i++) {
            int radix = radices[i];
            stringBuilder.append(radix);
            stringBuilder.append(",");
        }
        
        return StringUtils.stripEnd(stringBuilder.toString(), ",");
    }
    
    public static int[] deserializeRadices(String serializedRadices) {
        String[] intStrs = serializedRadices.split(",");
        int[] radices = new int[intStrs.length];
        for(int i=0; i < intStrs.length; i++) {
            radices[i] = Integer.parseInt(intStrs[i]);
        }
        
        return radices;
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.core.data.*;
import ethier.alex.world.mapreduce.data.BigDecimalWritable;
import ethier.alex.world.mapreduce.data.ElementListWritable;
import ethier.alex.world.mapreduce.memory.HdfsMemoryManager;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**

 @author alex
 */
public class QueryMapper extends Mapper<Text, ElementListWritable, Text, Writable> {

    private static Logger logger = Logger.getLogger(QueryMapper.class);
    private Collection<FilterList> filters;
    private int[] radices;

    @Override
    protected void setup(Context context) throws IOException {

        int mapperId = context.getTaskAttemptID().getTaskID().getId();
        logger.info("Setting up mapper: " + mapperId);
        try {
            String serializedFilters = HdfsMemoryManager.getString(QueryRunner.MEMORY_FILTERS_NAME, context.getConfiguration());
            String serializedRadices = HdfsMemoryManager.getString(QueryRunner.MEMORY_RADICES_NAME, context.getConfiguration());
            filters = FilterList.deserializeFilters(serializedFilters);
            radices = Partition.deserializeRadices(serializedRadices);
        } catch (DecoderException ex) {
            throw new RuntimeException("Unable to setup query mapper.  Caused by: " + ExceptionUtils.getFullStackTrace(ex));
        }
        logger.info("Setup complete.");
    }

    @Override
    public void map(Text key, ElementListWritable value, Context context) throws IOException, InterruptedException {

        ElementList elementList = value.getElementList();
        
        BigDecimal weightedMatch = this.getWeightedMatch(filters, elementList);
        logger.info("Element List: " + elementList + " has weight: " + weightedMatch.toPlainString());
        context.write(key, new BigDecimalWritable(weightedMatch));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        outputWriter.close();
    }

    private BigDecimal getWeightedMatch(Collection<FilterList> filterLists, ElementList elementList) {

        BigDecimal weight = BigDecimal.valueOf(1L);

        for (int i = 0; i < radices.length; i = i + 1) {

            Collection<Filter> compareFilters = new ArrayList<Filter>();
            for (FilterList filterList : filterLists) {
                compareFilters.add(filterList.getFilter(i));
            }

            int elementWeight = getUnionMaxWeight(compareFilters, elementList.getElement(i), i);

            if (elementWeight == 0) {
                return BigDecimal.valueOf(0L);
            } else {
                weight = weight.multiply(BigDecimal.valueOf(elementWeight));
            }
        }

        return weight;
    }

    private int getUnionMaxWeight(Collection<Filter> filters, Element element, int matchPos) {

        boolean allFilterPresent = false;
        Set<Integer> filterOrdinals = new HashSet<Integer>();
        for (Filter filter : filters) {

            if (filter.getFilterState() == FilterState.ALL) {
                allFilterPresent = true;
                break;
            }

            filterOrdinals.add(filter.getOrdinal());
        }

        if (allFilterPresent && element.getElementState() == ElementState.ALL) {
            return radices[matchPos];
        } else if (allFilterPresent && element.getElementState() == ElementState.SET) {
            return 1;
        } else if (!allFilterPresent && element.getElementState() == ElementState.ALL) {
            return filterOrdinals.size();
        } else if (!allFilterPresent && element.getElementState() == ElementState.SET) {
            for (int filterOrdinal : filterOrdinals) {
                if (filterOrdinal == element.getOrdinal()) {
                    return 1;
                }
            }

            return 0;
        } else {
            throw new RuntimeException("Invalid State During Query Pos: + " + matchPos
                    + " Element State: " + element.getElementState()
                    + " Filters: " + filters);
        }
    }
}

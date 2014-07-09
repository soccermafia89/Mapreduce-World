/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.query;

import ethier.alex.world.core.data.FilterList;
import ethier.alex.world.query.Query;
import java.util.Collection;

/**

 @author alex
 */
public class DistributedQuery implements Query {
    
    public DistributedQuery() {
        
    }

    @Override
    public long getWorldSize() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double query(FilterList filter) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double query(Collection<FilterList> filters) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}

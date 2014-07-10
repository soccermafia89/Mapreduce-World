/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce.data;

//import ethier.alex.world.core.data.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import org.apache.hadoop.io.Writable;


/**

 @author alex
 */
public class BigDecimalWritable implements Writable {
    
    BigDecimal bigDecimal;
    
    public BigDecimalWritable() {
        
    }
    
    public BigDecimalWritable(DataInput in) throws IOException {
        this.readFields(in);
    }
    
    public BigDecimalWritable(BigDecimal myBigDecimal) {
        bigDecimal = myBigDecimal;
    }
    
    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = bigDecimal.toPlainString().getBytes(Charset.defaultCharset());
        int length = bytes.length;
        out.writeInt(length);
        for(int i=0; i < length;i++) {
            out.write(bytes[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        
        byte[] bytes = new byte[length];
        for(int i=0; i < length; i++) {
            bytes[i] = in.readByte();
        }
        
        String plainBigDecimalStr = new String(bytes, Charset.defaultCharset());
        bigDecimal = new BigDecimal(plainBigDecimalStr);
    }
    
}

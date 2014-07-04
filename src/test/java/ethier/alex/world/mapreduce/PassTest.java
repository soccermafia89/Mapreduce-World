/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ethier.alex.world.mapreduce;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**

 @author alex
 */
public class PassTest {
    
    private static Logger logger = Logger.getLogger(PassTest.class);
    
    @BeforeClass
    public static void setUpClass() {
        BasicConfigurator.configure();
    }
    
    @Test
    public void testPass() throws Exception {
        System.out.println("");
        System.out.println("");
        System.out.println("********************************************");
        System.out.println("********         Pass  Test        *********");
        System.out.println("********************************************");
        System.out.println("");
        System.out.println(""); 
        
        Assert.assertTrue(true);
    }
}

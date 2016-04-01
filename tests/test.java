import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import wrapper.SmartSpaceException;
import wrapper.SmartSpaceKPI;
import wrapper.SmartSpaceTriple;

import java.util.Vector;

/**
 * Created by user on 29.03.16.
 */
public class test {
    static SmartSpaceKPI smartSpaceKPI;
    private final static boolean SSOnRouter = true;

    @BeforeClass
    public static void connect() {
        String routerHost = "192.168.1.1", PCHost = "192.168.2.101";
        try {
            smartSpaceKPI = new SmartSpaceKPI(SSOnRouter ? routerHost : PCHost, 10010, "x");
        } catch (SmartSpaceException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void exit() {
        if (smartSpaceKPI != null)
            try {
                smartSpaceKPI.leave();
            } catch (SmartSpaceException e) {
                e.printStackTrace();
            }
    }

    private boolean triplesEquals(SmartSpaceTriple triple1, SmartSpaceTriple triple2) {
        return triple1.getSubject().equals(triple2.getSubject())
                && triple1.getPredicate().equals(triple2.getPredicate())
                && triple1.getObject().equals(triple2.getObject());
    }

    @Test
    public void insertTest() {
        if (smartSpaceKPI != null) {
            SmartSpaceTriple testTriple = new SmartSpaceTriple("testSubject", "testPredicate", "testObject");
            Vector<SmartSpaceTriple> result = new Vector<SmartSpaceTriple>();
            try {
                smartSpaceKPI.remove(testTriple);
                smartSpaceKPI.insert(testTriple);
                result = smartSpaceKPI.query(testTriple);
            } catch (SmartSpaceException e) {
                e.printStackTrace();
            }
            assert result.size() > 0;
            assert triplesEquals(result.elementAt(0), testTriple);
        }
    }
}

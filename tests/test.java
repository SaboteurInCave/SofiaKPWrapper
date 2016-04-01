import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import sofia_kp.SSAP_sparql_response;
import sofia_kp.iKPIC_subscribeHandler2;
import wrapper.SmartSpaceException;
import wrapper.SmartSpaceKPI;
import wrapper.SmartSpaceTriple;

import java.util.Vector;

/**
 * Created by user on 29.03.16.
 */
public class test implements iKPIC_subscribeHandler2 {
    static SmartSpaceKPI smartSpaceKPI;
    static SmartSpaceTriple testTriple;
    static SmartSpaceTriple testTriple2;
    static Vector<SmartSpaceTriple> result;
    private final static boolean SSOnRouter = true;

    @BeforeClass
    public static void connect() {
        String routerHost = "192.168.1.1", PCHost = "192.168.2.101";
        testTriple = new SmartSpaceTriple("testSubject", "testPredicate", "testObject");
        testTriple2 = new SmartSpaceTriple("testSubject2", "testPredicate2", "testObject2");
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

    private boolean insert(SmartSpaceTriple insertedTriple) {
        Vector<SmartSpaceTriple> result = new Vector<SmartSpaceTriple>();
        try {
            smartSpaceKPI.remove(testTriple);
            smartSpaceKPI.insert(testTriple);
            result = smartSpaceKPI.query(testTriple);
        } catch (SmartSpaceException e) {
            e.printStackTrace();
        }
        return result.size() > 0 && triplesEquals(result.elementAt(0), testTriple);
    }

    @Test
    public void insertTest() {
        if (smartSpaceKPI != null) {
            assert insert(testTriple);
        }
    }

    @Test
    public void removeTest() {
        if (smartSpaceKPI != null) {
            if (insert(testTriple)) {
                try {
                    smartSpaceKPI.remove(new SmartSpaceTriple(null, "commandIs", null));
                    smartSpaceKPI.remove(testTriple);
                    result = smartSpaceKPI.query(testTriple);
                } catch (SmartSpaceException e) {
                    e.printStackTrace();
                }
                assert result.size() == 0;
            }
        }
    }

    @Test
    public void subscribeTest() {
        if (smartSpaceKPI != null) {
            try {
                result = new Vector<SmartSpaceTriple>();
                smartSpaceKPI.subscribe(testTriple, this);
                if (insert(testTriple)) {
                    Thread.sleep(2000);
                    assert result.size() > 0;
                    assert triplesEquals(result.elementAt(0), testTriple);
                }
            } catch (SmartSpaceException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void unsubscribeTest() {
        if (smartSpaceKPI != null) {
            try {
                result = new Vector<SmartSpaceTriple>();
                smartSpaceKPI.unsubscribe(testTriple2, true);
                smartSpaceKPI.subscribe(testTriple2, this);
                smartSpaceKPI.unsubscribe(testTriple2, true);
                if (insert(testTriple2)) {
                    Thread.sleep(2000);
                    assert result.size() == 0;
                }
            } catch (SmartSpaceException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void kpic_RDFEventHandler(Vector<Vector<String>> vector, Vector<Vector<String>> vector1, String s, String s1) {
        result = new Vector<SmartSpaceTriple>();
        for (Vector<String> tripleVector : vector)
            result.add(new SmartSpaceTriple(tripleVector.elementAt(0), tripleVector.elementAt(1), tripleVector.elementAt(2)));
    }

    @Override
    public void kpic_SPARQLEventHandler(SSAP_sparql_response ssap_sparql_response, SSAP_sparql_response ssap_sparql_response1, String s, String s1) {

    }

    @Override
    public void kpic_UnsubscribeEventHandler(String s) {

    }

    @Override
    public void kpic_ExceptionEventHandler(Throwable throwable) {

    }
}

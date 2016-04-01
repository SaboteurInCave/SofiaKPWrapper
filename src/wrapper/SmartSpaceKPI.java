package wrapper;

import sofia_kp.SIBResponse;
import sofia_kp.iKPIC_subscribeHandler2;
import sofia_kp.KPICore;

import java.util.ArrayList;
import java.util.Vector;

public class SmartSpaceKPI {

    private KPICore core;
    private ArrayList<String> subscriptionIdList;
    private ArrayList<SmartSpaceTriple> tripleList;

    public SmartSpaceKPI(String host, int port, String spaceName) throws SmartSpaceException {
        core = new KPICore(host, port, spaceName);
        subscriptionIdList = new ArrayList<String>();
        tripleList = new ArrayList<SmartSpaceTriple>();

        SIBResponse joinResponse = core.join();

        if (!joinResponse.isConfirmed())
            throw new SmartSpaceException(joinResponse.Message);

        core.disable_debug_message();
    }

    public void insert(SmartSpaceTriple triple) throws SmartSpaceException {
        SIBResponse insertResponse = core.insert(triple.getSubject(), triple.getPredicate(), triple.getObject(), triple.getSubjectType(), triple.getObjectType());
        if (!insertResponse.isConfirmed()) {
            String text = String.format("KPI failed to insert triple: (%s, %s, %s, %s, %s)",
                    triple.getSubject(), triple.getPredicate(), triple.getObject(),
                    triple.getSubjectType(), triple.getObjectType());

            throw new SmartSpaceException(text + '\n' + insertResponse.Message);
        }
    }

    public void remove(SmartSpaceTriple triple) throws SmartSpaceException {
        SIBResponse removeResponse = core.remove(triple.getSubject(), triple.getPredicate(), triple.getObject(), triple.getSubjectType(), triple.getObjectType());
        if (!removeResponse.isConfirmed()) {
            String text = String.format("KP failed to remove triple: (%s, %s, %s, %s, %s)",
                    triple.getSubject(), triple.getPredicate(), triple.getObject(),
                    triple.getSubjectType(), triple.getObjectType());

            throw new SmartSpaceException(text + '\n' + removeResponse.Message);
        }
    }

    public Vector<SmartSpaceTriple> query(SmartSpaceTriple triple) throws SmartSpaceException {
        SIBResponse queryResponse = core.queryRDF(triple.getSubject(), triple.getPredicate(), triple.getObject(), triple.getSubjectType(), triple.getObjectType());

        if (queryResponse.isConfirmed()) {
            Vector<Vector<String>> stringVectorResult = queryResponse.query_results;

            Vector<SmartSpaceTriple> result = new Vector<SmartSpaceTriple>();

            for (Vector<String> it : stringVectorResult) {
                result.add(new SmartSpaceTriple(it));
            }

            return result;
        } else {
            throw new SmartSpaceException(queryResponse.Message);
        }
    }

    public void update(SmartSpaceTriple newTriple, SmartSpaceTriple oldTriple) throws SmartSpaceException {
        SIBResponse updateResponse = core.update(
                newTriple.getSubject(), newTriple.getPredicate(), newTriple.getObject(), newTriple.getSubjectType(), newTriple.getObjectType(),
                oldTriple.getSubject(), oldTriple.getPredicate(), oldTriple.getObject(), oldTriple.getSubjectType(), oldTriple.getObjectType()
        );

        if (!updateResponse.isConfirmed()) {
            String text = String.format("KP failed to update triple! Old triple: (%s, %s, %s, %s, %s), new triple (%s, %s, %s, %s, %s)",
                    newTriple.getSubject(), newTriple.getPredicate(), newTriple.getObject(), newTriple.getSubjectType(), newTriple.getObjectType(),
                    oldTriple.getSubject(), oldTriple.getPredicate(), oldTriple.getObject(), oldTriple.getSubjectType(), oldTriple.getObjectType());

            throw new SmartSpaceException(text + '\n' + updateResponse.Message);
        }
    }

    public String subscribe(SmartSpaceTriple triple, iKPIC_subscribeHandler2 handler) throws SmartSpaceException {
        SIBResponse subscribeResponse = core.subscribeRDF(triple.getSubject(), triple.getPredicate(), triple.getObject(), triple.getObjectType(), handler);

        if (subscribeResponse != null && subscribeResponse.isConfirmed()) {
            subscriptionIdList.add(subscribeResponse.subscription_id);
            tripleList.add(triple);
            return subscribeResponse.subscription_id;
        } else {
            System.err.println("Some problems with subscribing");
            throw new SmartSpaceException(subscribeResponse != null ? subscribeResponse.Message : null);
        }
    }

    public void leave() throws SmartSpaceException {

        try {
            unsubscribe();
        } catch (SmartSpaceException exception) {
            System.err.println(exception.getMessage());
        }

        SIBResponse leaveResponse = core.leave();

        if (!leaveResponse.isConfirmed()) {
            throw new SmartSpaceException(leaveResponse.Message);
        }
    }


    private void unsubscribe() throws SmartSpaceException {
        String exceptionMessage = "";

        for (String id : subscriptionIdList) {
            SIBResponse unsubscribeResponse = core.unsubscribe(id);

            // у нас проблемы с отпиской от интеллектуального пространства
            if (!unsubscribeResponse.isConfirmed()) {
                exceptionMessage += id + ": " + unsubscribeResponse.Message + '\n';
            }
        }

        subscriptionIdList.clear();

        // проблемы во время отписки были, сигнализируем это
        if (!exceptionMessage.isEmpty()) {
            throw new SmartSpaceException(exceptionMessage);
        }
    }


    public void unsubscribe(Iterable<String> subscriptionIds) throws SmartSpaceException {
        String exceptionMessage = "";

        for (String subscriptionId : subscriptionIds) {
            try {
                unsubscribe(subscriptionId);
            } catch (SmartSpaceException e) {
                exceptionMessage += e.getMessage() + "\n";
            }
        }

        // проблемы во время отписки были, сигнализируем это
        if (!exceptionMessage.isEmpty()) {
            throw new SmartSpaceException(exceptionMessage);
        }
    }

    public void unsubscribe(String subscriptionId) throws SmartSpaceException {
        SIBResponse unsubscribeResponse = core.unsubscribe(subscriptionId);

        // у нас проблемы с отпиской от интеллектуального пространства
        if (!unsubscribeResponse.isConfirmed()) {
            throw new SmartSpaceException(subscriptionId + ": " + unsubscribeResponse.Message + '\n');
        } else {
            int index = subscriptionIdList.indexOf(subscriptionId);
            if (index >= 0) {
                subscriptionIdList.remove(index);
                tripleList.remove(index);
            }
        }
    }

    public void unsubscribe(Iterable<SmartSpaceTriple> triples, boolean fullMatch) throws SmartSpaceException {
        String exceptionMessage = "";

        for (SmartSpaceTriple triple : triples)
            try {
                unsubscribe(triple, fullMatch);
            } catch (SmartSpaceException e) {
                exceptionMessage += e.getMessage() + "\n";
            }

        // проблемы во время отписки были, сигнализируем это
        if (!exceptionMessage.isEmpty()) {
            throw new SmartSpaceException(exceptionMessage);
        }
    }

    public void unsubscribe(SmartSpaceTriple triple, boolean fullMatch) throws SmartSpaceException {
        String exceptionMessage = "";

        String subject = triple.getSubject(), predicate = triple.getPredicate(), object = triple.getObject();

        ArrayList<String> oldIdList = new ArrayList<String>(subscriptionIdList);
        for (int i = oldIdList.size() - 1; i >= 0; i--) {
            SmartSpaceTriple curTriple = tripleList.get(i);

            if (checkTwoStrings(subject, curTriple.getSubject(), fullMatch) &&
                    checkTwoStrings(predicate, curTriple.getPredicate(), fullMatch) &&
                    checkTwoStrings(object, curTriple.getObject(), fullMatch)) {
                SIBResponse unsubscribeResponse = core.unsubscribe(oldIdList.get(i));

                // у нас проблемы с отпиской от интеллектуального пространства
                if (!unsubscribeResponse.isConfirmed()) {
                    exceptionMessage += oldIdList.get(i) + ": " + unsubscribeResponse.Message + '\n';
                } else {
                    subscriptionIdList.remove(i);
                    tripleList.remove(i);
                }
            }
        }

        // проблемы во время отписки были, сигнализируем это
        if (!exceptionMessage.isEmpty()) {
            throw new SmartSpaceException(exceptionMessage);
        }
    }

    private boolean checkTwoStrings(String mainString, String curString, boolean fullMatch) {
        if (mainString == null)
            return !fullMatch || (curString == null);
        return mainString.equals(curString);
    }
}

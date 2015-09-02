package wrapper;

import sofia_kp.SIBResponse;
import sofia_kp.iKPIC_subscribeHandler2;
import sofia_kp.KPICore;

import java.util.ArrayList;
import java.util.Vector;

public class SmartSpaceKPI {

    private KPICore core;
    private ArrayList<String> subscriptionIdList;

    public SmartSpaceKPI(String host, int port, String spaceName) throws SmartSpaceException {
        core = new KPICore(host, port, spaceName);
        subscriptionIdList = new ArrayList<String>();

        SIBResponse joinResponse = core.join();

        if (!joinResponse.isConfirmed())
            throw new SmartSpaceException(joinResponse.Message);

        core.disable_debug_message();
    }

    public void insert(SmartSpaceTriplet triplet) throws SmartSpaceException {
        SIBResponse insertResponse = core.insert(triplet.getSubject(), triplet.getPredicate(), triplet.getObject(), triplet.getSubjectType(), triplet.getObjectType());
        if (!insertResponse.isConfirmed()) {
            String text = String.format("KPI failed to insert triplet: (%s, %s, %s, %s, %s)",
                                        triplet.getSubject(), triplet.getPredicate(), triplet.getObject(),
                                        triplet.getSubjectType(), triplet.getObjectType());

            throw new SmartSpaceException(text + '\n' + insertResponse.Message);
        }
    }

    public void remove(SmartSpaceTriplet triplet) throws SmartSpaceException {
        SIBResponse removeResponse = core.remove(triplet.getSubject(), triplet.getPredicate(), triplet.getObject(), triplet.getSubjectType(), triplet.getObjectType());
        if (!removeResponse.isConfirmed()) {
            String text = String.format("KP failed to remove triplet: (%s, %s, %s, %s, %s)",
                                        triplet.getSubject(), triplet.getPredicate(), triplet.getObject(),
                                        triplet.getSubjectType(), triplet.getObjectType());

            throw new SmartSpaceException(text + '\n' + removeResponse.Message);
        }
    }

    public Vector<SmartSpaceTriplet> query(SmartSpaceTriplet triplet) throws SmartSpaceException {
        SIBResponse queryResponse = core.queryRDF(triplet.getSubject(), triplet.getPredicate(), triplet.getObject(), triplet.getSubjectType(), triplet.getObjectType());

        if (queryResponse.isConfirmed()) {
            Vector<Vector<String>> stringVectorResult = queryResponse.query_results;

            Vector<SmartSpaceTriplet> result = new Vector<SmartSpaceTriplet>();

            for (Vector<String> it : stringVectorResult) {
                result.add(new SmartSpaceTriplet(it));
            }

            return result;
        } else {
            throw new SmartSpaceException(queryResponse.Message);
        }
    }

    public void update(SmartSpaceTriplet newTriplet, SmartSpaceTriplet oldTriplet) throws SmartSpaceException {
        SIBResponse updateResponse = core.update(
                newTriplet.getSubject(), newTriplet.getPredicate(), newTriplet.getObject(), newTriplet.getSubjectType(), newTriplet.getObjectType(),
                oldTriplet.getSubject(), oldTriplet.getPredicate(), oldTriplet.getObject(), oldTriplet.getSubjectType(), oldTriplet.getObjectType()
        );

        if (!updateResponse.isConfirmed()) {
            String text = String.format("KP failed to update triplet! Old triplet: (%s, %s, %s, %s, %s), new triplet (%s, %s, %s, %s, %s)",
                    newTriplet.getSubject(), newTriplet.getPredicate(), newTriplet.getObject(), newTriplet.getSubjectType(), newTriplet.getObjectType(),
                    oldTriplet.getSubject(), oldTriplet.getPredicate(), oldTriplet.getObject(), oldTriplet.getSubjectType(), oldTriplet.getObjectType());

            throw new SmartSpaceException(text + '\n' + updateResponse.Message);
        }
    }

    public void subscribe(SmartSpaceTriplet triplet, iKPIC_subscribeHandler2 handler) throws SmartSpaceException {
        SIBResponse subscribeResponse = core.subscribeRDF(triplet.getSubject(), triplet.getPredicate(), triplet.getObject(), triplet.getObjectType(), handler);

        if (subscribeResponse != null && subscribeResponse.isConfirmed()) {
            subscriptionIdList.add(subscribeResponse.subscription_id);
        } else {
            System.err.println("Some problems with subscribing");
            throw new SmartSpaceException(subscribeResponse != null ? subscribeResponse.Message : null);
        }
    }

    public void leave() throws SmartSpaceException {

        try {
            unsubscribe();
        }
        catch (SmartSpaceException exception)
        {
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
}

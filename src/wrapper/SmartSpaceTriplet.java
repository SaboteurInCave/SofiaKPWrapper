package wrapper;

// Class for triplets

import java.util.Vector;

public class SmartSpaceTriplet {
    private String subject, predicate, object, subjectType, objectType;

    public SmartSpaceTriplet(String subject, String predicate, String object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.subjectType = "uri";
        this.objectType = "literal";
    }

    public SmartSpaceTriplet(String subject, String predicate, String object, String subjectType, String objectType) throws SmartSpaceException {

        subjectType.toLowerCase();
        objectType.toLowerCase();

        if (!subjectType.equals("uri") && !subjectType.equals("literal"))
            throw new SmartSpaceException("Subject type must have \"uri\" or \"literal\" type - got " + subjectType);

        if (!objectType.equals("uri") && !objectType.equals("literal"))
            throw new SmartSpaceException("Object type must have \"uri\" or \"literal\" type - got " + objectType);

        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.subjectType = subjectType;
        this.objectType = objectType;
    }

    public SmartSpaceTriplet(Vector<String> triplet) {
        switch (triplet.size()) {
            case 5: {
                this.subject = triplet.elementAt(0);
                this.predicate = triplet.elementAt(1);
                this.object = triplet.elementAt(2);
                this.subjectType = triplet.elementAt(3);
                this.objectType = triplet.elementAt(4);
            }
            case 4: {
                this.subject = triplet.elementAt(0);
                this.predicate = triplet.elementAt(1);
                this.object = triplet.elementAt(2);
                this.subjectType = "uri";
                this.objectType = triplet.elementAt(3);
            }
        }
    }

    public String getSubject() {
        return subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public String getObject() {
        return object;
    }

    public String getSubjectType() {
        return subjectType;
    }

    public String getObjectType() {
        return objectType;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public void setSubjectType(String subjectType) throws SmartSpaceException {
        subjectType.toLowerCase();

        if (!subjectType.equals("uri") || !subjectType.equals("literal"))
            throw new SmartSpaceException("Subject type must have \"uri\" or \"literal\" type - got " + subjectType);


        this.subjectType = subjectType;
    }

    public void setObjectType(String objectType) throws SmartSpaceException {
        objectType.toLowerCase();

        if (!objectType.equals("uri") || !objectType.equals("literal"))
            throw new SmartSpaceException("Object type must have \"uri\" or \"literal\" type - got " + objectType);

        this.objectType = objectType;
    }

    @Override
    public String toString() {
        return String.format("subject: %s, predicate: %s, object: %s, subjectType: %s, objectType: %s", subject, predicate, object, subjectType, objectType);
    }
}

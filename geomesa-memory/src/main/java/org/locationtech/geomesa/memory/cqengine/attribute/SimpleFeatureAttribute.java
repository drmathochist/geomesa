package org.locationtech.geomesa.memory.cqengine.attribute;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.opengis.feature.simple.SimpleFeature;

public class SimpleFeatureAttribute<A> extends SimpleAttribute<SimpleFeature, A> {

    String fieldName;

    public SimpleFeatureAttribute(Class<A> fieldType, String fieldName) {
        super(SimpleFeature.class, fieldType, fieldName);

        this.fieldName = fieldName;
    }

    @Override
    public A getValue(SimpleFeature object, QueryOptions queryOptions) {
        return (A) object.getAttribute(fieldName);
    }
}

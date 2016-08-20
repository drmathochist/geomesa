/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.index;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.AbstractAttributeIndex;
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex;
import com.googlecode.cqengine.persistence.support.ObjectSet;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.locationtech.geomesa.memory.cqengine.query.Intersects;
import org.locationtech.geomesa.utils.index.BucketIndex;
import org.opengis.feature.simple.SimpleFeature;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class GeoIndex<A extends Geometry, O extends SimpleFeature> extends AbstractAttributeIndex<A, O> implements OnHeapTypeIndex {

    private static final int INDEX_RETRIEVAL_COST = 40;

    volatile BucketIndex<SimpleFeature> index;

    static Set<Class<? extends Query>> supportedQueries = new HashSet<Class<? extends Query>>() {{
        add(Intersects.class);
    }};

    public GeoIndex(Attribute<O, A> attribute) {
        super(attribute, supportedQueries);
        index = new BucketIndex<SimpleFeature>(360, 180, new Envelope(-180.0, 180.0, -90.0, 90.0));
    }

    public static <A extends Geometry, O extends SimpleFeature> GeoIndex<A , O> onAttribute(Attribute<O, A> attribute) {
        return new GeoIndex<A, O>(attribute);
    }

    @Override
    public void init(ObjectStore<O> objectStore, QueryOptions queryOptions) {
        System.out.println("Called init in GeoIndex");
        addAll(ObjectSet.fromObjectStore(objectStore, queryOptions), queryOptions);
    }

    @Override
    public boolean addAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        System.out.println("In addAll for GeoIndex");
        try {
            boolean modified = false;

            for (O object : objectSet) {
                Envelope env = ((Geometry)object.getDefaultGeometry()).getEnvelopeInternal();
                //System.out.println("Adding " + object + " with envelope " + env);
                index.insert(env, object);
                modified = true;
            }

            return modified;
        }
        finally {
            objectSet.close();
        }
    }

    @Override
    public boolean removeAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        System.out.println("In removeAll for GeoIndex");
        try {
            boolean modified = false;

            for (O object : objectSet) {
                Envelope env = ((Geometry)object.getDefaultGeometry()).getEnvelopeInternal();
                System.out.println("Removing " + object + " with envelope " + env);
                index.remove(env, object);
                modified = true;
            }

            return modified;
        }
        finally {
            objectSet.close();
        }
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        System.out.println("Clearing the GeoIndex");
        this.index = new BucketIndex<SimpleFeature>(360, 180, new Envelope(-180.0, 180.0, -90.0, 90.0));
    }

    @Override
    public ResultSet<O> retrieve(final Query<O> query, final QueryOptions queryOptions) {
        return new ResultSet<O>() {
            @Override
            public Iterator<O> iterator() {
                System.out.println("In retrieve's iterator method");
                scala.collection.Iterator<SimpleFeature> iter = getSimpleFeatureIteratorInternal((Intersects) query, queryOptions);

                // JNH: Fix this:!?
                return (Iterator<O>) JavaConversions.asJavaIterator(iter);
            }

            @Override
            public boolean contains(O object) {
                System.out.println("In retrieve's contains method");
                return false;
            }

            @Override
            public boolean matches(O object) {
                System.out.println("In retrieve's matches method");
                return false;
            }

            @Override
            public Query<O> getQuery() {
                System.out.println("In retrieve's getQuery method");
                return query;
            }

            @Override
            public QueryOptions getQueryOptions() {
                System.out.println("In retrieve's getQueryOptions method");
                return queryOptions;
            }

            @Override
            public int getRetrievalCost() {
                System.out.println("In retrieve's getRetrievalCost method");
                return INDEX_RETRIEVAL_COST;
            }

            // JNH: I'm returning the size here as the MergeCost.
            // The geoindex size isn't optimal, so I don't know if this is good or not...
            @Override
            public int getMergeCost() {
                System.out.println("In retrieve's getMergeCost method");
                return size();
            }

            @Override
            public int size() {
                System.out.println("In retrieve's size method");
                return getSimpleFeatureIteratorInternal((Intersects) query, queryOptions).size();
            }

            @Override
            public void close() {

            }
        };
    }

    private scala.collection.Iterator<SimpleFeature> getSimpleFeatureIteratorInternal(Intersects query, final QueryOptions queryOptions) {
        final Intersects intersects = query;
        Envelope queryEnvelope = intersects.getEnvelope();
        return index.query(queryEnvelope, new AbstractFunction1<SimpleFeature, Object>() {
            @Override
            public Object apply(SimpleFeature feature) {
                String attributeName = intersects.getAttributeName();

                try {
                    Geometry geom = (Geometry) feature.getAttribute(attributeName);
                    return intersects.matchesValue(geom, queryOptions);
                } catch (Exception e) {
                    System.out.println("Caught exception while trying to look up geometry.");
                    e.printStackTrace();
                    return false;
                }
            }
        });
    }


    @Override
    public boolean isMutable() {
        return true;
    }

    // JNH: I don't think we have to quantize this index.
    @Override
    public boolean isQuantized() {
        return false;
    }

    @Override
    public Index<O> getEffectiveIndex() {
        return this;
    }
}
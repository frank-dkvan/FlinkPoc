package com.data.source;


import com.clicktale.data.PageViewRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Created by Gennady.Gilin on 6/29/2016.
 */
public class PageViewRecordSchema implements DeserializationSchema<PageViewRecord>, SerializationSchema<PageViewRecord> {


    private static final long serialVersionUID = 1L;

    @Override
    public PageViewRecord deserialize(byte[] message) {
        return PageViewRecord.fromString( new String( message ));
    }

    @Override
    public boolean isEndOfStream( PageViewRecord nextElement) {
        return false;
    }

    @Override
    public byte[] serialize( PageViewRecord element) {
        return element.toString().getBytes();
    }

    @Override
    public TypeInformation<PageViewRecord> getProducedType() {

        return TypeExtractor.getForClass(PageViewRecord.class);
    }
}



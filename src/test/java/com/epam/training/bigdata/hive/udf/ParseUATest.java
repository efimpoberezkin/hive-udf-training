package com.epam.training.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParseUATest {



    @Before

    @Test
    public void testParseUAReturnsCorrectValues() throws HiveException {

        // set up the models we need
        ParseUA udf = new ParseUA();

        StringObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        List<String> outputFieldNames = Arrays.asList("device", "os", "browser", "browser_group", "type");
        List<ObjectInspector> outputInspectors = Arrays.asList(outputOI, outputOI, outputOI, outputOI, outputOI);

        StructObjectInspector resultInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors);

        // create actual UDF argument
        String argument = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)";

        // test result
        Object result = udf.evaluate(new DeferredObject[]{new GenericUDF.DeferredJavaObject(argument)});
        List<Object> resultStruct = resultInspector.getStructFieldsDataAsList(result);

        assertEquals(5, resultStruct.size());
        assertEquals("Computer", resultStruct.get(0).toString());
        assertEquals("Windows XP", resultStruct.get(1).toString());
        assertEquals("Internet Explorer 6", resultStruct.get(2).toString());
        assertEquals("Internet Explorer", resultStruct.get(3).toString());
        assertEquals("Browser", resultStruct.get(4).toString());
    }

    @Test(expected = HiveException.class)
    public void testParseUANullArgument() throws HiveException {

        new ParseUA().evaluate(new DeferredObject[]{new GenericUDF.DeferredJavaObject(null)});
    }
}

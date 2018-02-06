package com.epam.training.bigdata.hive.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;

/**
 * UDF that parses UA string into separate fields: device, os, browser, type.
 */
public class ParseUA extends GenericUDF {

    @Override
    public String getDisplayString(String[] args) {
        return "ParseUA(" + args[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) {

        StringObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        List<String> outputFieldNames = Arrays.asList("device", "os", "browser", "type");
        List<ObjectInspector> outputInspectors = Arrays.asList(outputOI, outputOI, outputOI, outputOI);

        return ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {

        if (args == null || args.length != 1) {
            throw new HiveException("Invalid number of arguments");
        }
        if (args[0].get() == null) {
            throw new HiveException("Argument contains null instead of object");
        }

        Object argObj = args[0].get();

        // get argument as String
        String argument;
        if (argObj instanceof String) {
            argument = (String) argObj;
        } else if (argObj instanceof LazyString) {
            argument = ((LazyString) argObj).getWritableObject().toString();
        } else {
            throw new HiveException("Argument is neither String nor LazyString, it is " + argObj.getClass().getCanonicalName());
        }

        // parse UA string and return struct, which is just an array of objects: Object[]
        return parseUAString(argument);
    }

    private Object parseUAString(String argument) {
        Object[] result = new Object[4];
        UserAgent ua = new UserAgent(argument);

        result[0] = new Text(ua.getOperatingSystem().getDeviceType().getName());
        result[1] = new Text(ua.getOperatingSystem().getName());
        result[2] = new Text(ua.getBrowser().getName());
        result[3] = new Text(ua.getBrowser().getBrowserType().getName());

        return result;
    }
}

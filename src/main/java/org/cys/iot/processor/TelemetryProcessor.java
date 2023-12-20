package org.cys.iot.processor;

import org.json.JSONArray;

import java.util.Map;
import java.util.TreeMap;

public class TelemetryProcessor {
    static Map<String, Object> getValues(JSONArray values, String fliedName) {
        Map<String, Object> data = new TreeMap<>();
        data.put(fliedName + "1", convertFromInt16ToFloat32((int) values.get(0), (int) values.get(1)));
        data.put(fliedName + "2", convertFromInt16ToFloat32((int) values.get(2), (int) values.get(3)));
        data.put(fliedName + "3", convertFromInt16ToFloat32((int) values.get(4), (int) values.get(5)));
        data.put(fliedName + "AVG", convertFromInt16ToFloat32((int) values.get(6), (int) values.get(7)));
        return data;
    }

    static Map<String, Object> activeEnergyValues(JSONArray values, String fliedName) {
        Map<String, Object> data = new TreeMap<>();
        data.put(fliedName + "1", (float) ((int) values.get(1)));
        data.put(fliedName + "2", (float) ((int) values.get(3)));
        data.put(fliedName + "3", (float) ((int) values.get(5)));
        data.put(fliedName + "AVG", (float) ((int) values.get(7)));
        return data;
    }

    static float convertFromInt16ToFloat32(int value1, int value2) {

        String hex1 = Integer.toHexString(value1 & 0xFFFF);
        String hex2 = Integer.toHexString(value2 & 0xFFFF);

        String hexString = hex1 + hex2;

        long longValue = Long.parseLong(hexString, 16);
        float floatValue = Float.intBitsToFloat((int) longValue);
        floatValue = Math.round(floatValue * 100.0) / 100.0f;

        return floatValue;
    }

}

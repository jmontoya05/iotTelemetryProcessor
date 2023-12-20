package org.cys.iot.processor;

import org.cys.iot.cosmos.SyncMain;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import static org.cys.iot.processor.TelemetryProcessor.activeEnergyValues;
import static org.cys.iot.processor.TelemetryProcessor.getValues;

public class Main {
    public static void run(JSONObject body) {

        Map<String, Object> data = new TreeMap<>();

        Map<String, String> fieldOperations = getStringStringMap();

        fieldOperations.keySet().stream()
                .filter(fieldName -> !body.isNull(fieldName))
                .forEach(fieldName -> {
                    if ("activeEnergy".equals(fieldName)) {
                        data.putAll(activeEnergyValues(body.getJSONArray(fieldName), "activeEnergy"));
                    } else if ("temperature".equals(fieldName)) {
                        data.put("temperature", body.getFloat("temperature"));
                    } else if ("humidity".equals(fieldName)) {
                        data.put("humidity", body.getFloat("humidity"));
                    } else if ("pressure".equals(fieldName)) {
                        data.put("pressure", body.getFloat("pressure"));
                    } else {
                        data.putAll(getValues(body.getJSONArray(fieldName), fieldOperations.get(fieldName)));
                    }
                });

        String device = body.getString("device");
        ZoneId zone = ZoneId.of("UTC-5");
        ZonedDateTime utc5Time = ZonedDateTime.now(zone);
        LocalDateTime date = utc5Time.toLocalDateTime();
        String id = device + " - " + date;
        data.put("date", date.toString());
        data.put("device", device);
        data.put("id", id);
        data.put("partitionKey", id);

        SyncMain.publishDataToCosmos(data);

    }

    private static Map<String, String> getStringStringMap() {
        Map<String, String> fieldOperations = new HashMap<>();
        fieldOperations.put("power", "power");
        fieldOperations.put("powerFactor", "powerFactor");
        fieldOperations.put("voltage", "voltage");
        fieldOperations.put("voltageHarmonics", "voltageHarmonics");
        fieldOperations.put("current", "current");
        fieldOperations.put("currentHarmonics", "currentHarmonics");
        fieldOperations.put("activeEnergy", "activeEnergy");
        fieldOperations.put("temperature", "temperature");
        fieldOperations.put("humidity", "humidity");
        fieldOperations.put("pressure", "pressure");
        return fieldOperations;
    }
}

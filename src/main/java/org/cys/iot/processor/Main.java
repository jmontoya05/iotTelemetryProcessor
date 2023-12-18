package org.cys.iot.processor;

import org.cys.iot.cosmos.SyncMain;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.cys.iot.processor.TelemetryProcessor.activeEnergyValues;
import static org.cys.iot.processor.TelemetryProcessor.data;
import static org.cys.iot.processor.TelemetryProcessor.getValues;

public class Main {
    public static void run(JSONObject body) {

        data = new HashMap<>();

        Map<String, String> fieldOperations = new HashMap<>();
        fieldOperations.put("power", "Power");
        fieldOperations.put("powerFactor", "PowerFactor");
        fieldOperations.put("voltage", "Voltage");
        fieldOperations.put("voltageHarmonics", "VoltageHarmonics");
        fieldOperations.put("current", "Current");
        fieldOperations.put("currentHarmonics", "CurrentHarmonics");
        fieldOperations.put("activeEnergy", "ActiveEnergy");

        fieldOperations.keySet().stream()
                .filter(fieldName -> !body.isNull(fieldName))
                .forEach(fieldName -> {
                    if ("activeEnergy".equals(fieldName)) {
                        activeEnergyValues(body.getJSONArray(fieldName));
                    } else {
                        getValues(body.getJSONArray(fieldName), fieldOperations.get(fieldName));
                    }
                });

        String device = body.getString("device");
        System.out.println(device);
        String id = device + " - " + LocalDateTime.now();
        data.put("device", device);
        data.put("id", id);
        data.put("partitionKey", id);

        SyncMain.publishDataToCosmos(data);

    }
}

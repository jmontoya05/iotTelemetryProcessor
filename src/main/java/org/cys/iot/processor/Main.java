package org.cys.iot.processor;

import org.json.JSONObject;

import java.util.HashMap;

import static org.cys.iot.processor.TelemetryProcessor.*;

public class Main {
    public static void run(JSONObject body) {

        data = new HashMap<>();
        getValues(body.getJSONArray("power"), "Power");
        getValues(body.getJSONArray("powerFactor"), "PowerFactor");
        getValues(body.getJSONArray("voltage"), "Voltage");
        getValues(body.getJSONArray("voltageHarmonics"), "VoltageHarmonics");
        getValues(body.getJSONArray("current"), "Current");
        getValues(body.getJSONArray("currentArmonics"), "CurrentHarmonics");
        activeEnergyValues(body.getJSONArray("activeEnergy"));

        System.out.println(data);

    }
}

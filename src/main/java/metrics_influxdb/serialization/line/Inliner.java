package metrics_influxdb.serialization.line;

import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.Collectors;

import metrics_influxdb.measurements.Measure;
import metrics_influxdb.misc.Miscellaneous;

public class Inliner {
	public String inline(Measure m) {
		String key = buildMeasureKey(m.getName(), m.getTags());
            final Map<String, String> valuesMap = m.getValues().entrySet().stream()
                    .filter(entry->!"NaN".equals(entry.getValue()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue));
                
		String values = buildMeasureFields(valuesMap);

		return key + " " + values +  " " + BigDecimal.valueOf(m.getTimestamp()).multiply(BigDecimal.valueOf(1000000));
	}

	public String inline(Iterable<Measure> measures) {
		StringBuilder sb = new StringBuilder();
		String join = "";
		String cr = "\n";
		for (Measure m : measures) {
			sb.append(join).append(inline(m));
			join = cr;
		}
		return sb.toString();
	}

	private String buildMeasureFields(Map<String, String> values) {
		Map<String, String> sortedValues = new InfluxDBSortedMap();
		sortedValues.putAll(values);

		StringBuilder fields = new StringBuilder();
		String join = "";

		for (Map.Entry<String, String> v: sortedValues.entrySet()) {
			fields.append(join).append(Miscellaneous.escape(v.getKey(), ' ', ',')).append("=").append(v.getValue());		// values are already escaped
			join = ",";
		}
		return fields.toString();
	}

	private String buildMeasureKey(String name, Map<String, String> tags) {
		StringBuilder key = new StringBuilder(Miscellaneous.escape(name, ' ', ','));
		Map<String, String> sortedTags = new InfluxDBSortedMap();
		sortedTags.putAll(tags);

		for (Map.Entry<String, String> e: sortedTags.entrySet()) {
			key.append(',').append(Miscellaneous.escape(e.getKey(), ' ', ',')).append("=").append(Miscellaneous.escape(e.getValue(), ' ', ','));
		}

		return key.toString();
	}

}

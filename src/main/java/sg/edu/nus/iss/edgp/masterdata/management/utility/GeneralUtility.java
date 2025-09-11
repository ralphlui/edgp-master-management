package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import sg.edu.nus.iss.edgp.masterdata.management.enums.FileProcessStage;
import sg.edu.nus.iss.edgp.masterdata.management.service.impl.MasterdataService;
import sg.edu.nus.iss.edgp.masterdata.management.utility.GeneralUtility.BuiltUpdate;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Component
public class GeneralUtility {

	public static String makeNotNull(Object str) {
		if (str == null) {
			return "";
		} else if (str.equals("null")) {
			return "";
		} else {
			return str.toString();
		}
	}

	public static String nowSgt() {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		return LocalDateTime.now(ZoneId.of("Asia/Singapore")).format(fmt);
	}

	@lombok.Value
	public class BuiltUpdate {
		public Map<String, String> ean;
		public Map<String, AttributeValue> eav;
		public List<String> setParts;
		public int updatedFields;
	}

	public BuiltUpdate buildStagingUpdateParts(Map<String, Object> payload, Map<String, AttributeValue> current) {
		Map<String, String> ean = new LinkedHashMap<>();
		Map<String, AttributeValue> eav = new LinkedHashMap<>();
		List<String> setParts = new ArrayList<>();
		int updatedFields = 0;
		int idx = 0;

		for (Map.Entry<String, Object> entry : payload.entrySet()) {
			String field = entry.getKey();
			if ("id".equals(field))
				continue;
			if (!current.containsKey(field))
				continue;
			Object raw = entry.getValue();
			if (raw == null)
				continue;

			AttributeValue target = toAttr(raw);
			AttributeValue existing = current.get(field);
			if (equalsAttr(existing, target))
				continue;

			String n = "#n" + idx, v = ":v" + idx;
			ean.put(n, field);
			eav.put(v, target);
			setParts.add(n + " = " + v);
			updatedFields++;
			idx++;
		}

       // Resets (only if attribute exists)
		if (current.containsKey("processed_at")) {
			ean.put("#processed_at", "processed_at");
			eav.put(":processedEmpty", AttributeValue.builder().s("").build());
			setParts.add("#processed_at = :processedEmpty");
			updatedFields++;
		}
		if (current.containsKey("is_processed")) {
			ean.put("#is_processed", "is_processed");
			eav.put(":zeroProcessed", AttributeValue.builder().n("0").build());
			setParts.add("#is_processed = :zeroProcessed");
			updatedFields++;
		}
		if (current.containsKey("claimed_at")) {
			ean.put("#claimed_at", "claimed_at");
			eav.put(":claimedEmpty", AttributeValue.builder().s("").build());
			setParts.add("#claimed_at = :claimedEmpty");
			updatedFields++;
		}
		if (current.containsKey("is_handled")) {
			ean.put("#is_handled", "is_handled");
			eav.put(":zeroHandled", AttributeValue.builder().n("0").build());
			setParts.add("#is_handled = :zeroHandled");
			updatedFields++;
		}
		if (current.containsKey("updated_date")) {
			ean.put("#updated_date", "updated_date");
			eav.put(":now", AttributeValue.builder().s(GeneralUtility.nowSgt()).build());
			setParts.add("#updated_date = :now");
			updatedFields++;
		}

		return new BuiltUpdate(ean, eav, setParts, updatedFields);
	}

	public BuiltUpdate buildHeaderUpdateParts(Map<String, Object> payload, Map<String, AttributeValue> current) {
		Map<String, String> ean = new LinkedHashMap<>();
		Map<String, AttributeValue> eav = new LinkedHashMap<>();
		List<String> setParts = new ArrayList<>();

		int updatedFields = 0;
		int idx = 0;

		for (Map.Entry<String, Object> entry : payload.entrySet()) {
			String field = entry.getKey();
			if ("id".equals(field))
				continue;
			if (!current.containsKey(field))
				continue;
			Object raw = entry.getValue();
			if (raw == null)
				continue;

			AttributeValue target = toAttr(raw);
			AttributeValue existing = current.get(field);
			if (equalsAttr(existing, target))
				continue;

			String n = "#n" + idx, v = ":v" + idx;
			ean.put(n, field);
			eav.put(v, target);
			setParts.add(n + " = " + v);
			updatedFields++;
			idx++;
		}

        // Resets (only if attribute exists)
		if (current.containsKey("file_status")) {
			ean.put("#file_status", "file_status");
			eav.put(":fileStatusEmpty", AttributeValue.builder().s("").build());
			setParts.add("#file_status = :fileStatusEmpty");
			updatedFields++;
		}
		if (current.containsKey("is_processed")) {
			ean.put("#is_processed", "is_processed");
			eav.put(":zeroProcessed", AttributeValue.builder().n("0").build());
			setParts.add("#is_processed = :zeroProcessed");
			updatedFields++;
		}
		if (current.containsKey("process_stage")) {
			ean.put("#process_stage", "process_stage");
			eav.put(":processStageEmpty", AttributeValue.builder().s(FileProcessStage.UNPROCESSED.toString()).build());
			setParts.add("#process_stage = :processStageEmpty");
			updatedFields++;
		}
		if (current.containsKey("updated_date")) {
			ean.put("#updated_date", "updated_date");
			eav.put(":now", AttributeValue.builder().s(GeneralUtility.nowSgt()).build());
			setParts.add("#updated_date = :now");
			updatedFields++;
		}

		return new BuiltUpdate(ean, eav, setParts, updatedFields);
	}

	public BuiltUpdate buildRollbackFromSnapshot(Map<String, Object> before, Map<String, AttributeValue> afterAttrs) {
		Map<String, String> ean = new LinkedHashMap<>();
		Map<String, AttributeValue> eav = new LinkedHashMap<>();
		List<String> setParts = new ArrayList<>();
		int idx = 0;

		Map<String, Object> after = fromAttrMap(afterAttrs);

		for (Map.Entry<String, Object> e : before.entrySet()) {
			String field = e.getKey();
			if ("id".equals(field))
				continue;

			Object beforeVal = e.getValue();
			Object afterVal = after.get(field);

			if (Objects.equals(beforeVal, afterVal))
				continue;

			String n = "#rbn" + idx, v = ":rbv" + idx;
			ean.put(n, field);
			eav.put(v, toAttr(beforeVal));
			setParts.add(n + " = " + v);
			idx++;
		}
		return new BuiltUpdate(ean, eav, setParts, idx);
	}

	public static Map<String, String> merge(Map<String, String> a, Map<String, String> b) {
		Map<String, String> m = new LinkedHashMap<>(a);
		m.putAll(b);
		return m;
	}

	public  AttributeValue toAttr(Object v) {
		if (v == null)
			return AttributeValue.builder().nul(true).build();
		if (v instanceof Number)
			return AttributeValue.builder().n(String.valueOf(v)).build();
		if (v instanceof Boolean)
			return AttributeValue.builder().bool((Boolean) v).build();
		if (v instanceof Map)
			return AttributeValue.builder()
					.m(((Map<?, ?>) v).entrySet().stream()
							.collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> toAttr(e.getValue()))))
					.build();
		if (v instanceof List)
			return AttributeValue.builder().l(((List<?>) v).stream().map(this::toAttr).collect(Collectors.toList()))
					.build();
		return AttributeValue.builder().s(String.valueOf(v)).build();
	}

	public static boolean equalsAttr(AttributeValue a, AttributeValue b) {
		if (a == null && b == null)
			return true;
		if (a == null || b == null)
			return false;
		if (a.s() != null || b.s() != null)
			return Objects.equals(a.s(), b.s());
		if (a.n() != null || b.n() != null)
			return Objects.equals(a.n(), b.n());
		if (a.bool() != null || b.bool() != null)
			return Objects.equals(a.bool(), b.bool());
		if (a.hasM() || b.hasM())
			return Objects.equals(a.m(), b.m());
		if (a.hasL() || b.hasL())
			return Objects.equals(a.l(), b.l());
		if (a.nul() != null || b.nul() != null)
			return Objects.equals(a.nul(), b.nul());
		if (a.hasSs() || b.hasSs())
			return Objects.equals(a.ss(), b.ss());
		if (a.hasNs() || b.hasNs())
			return Objects.equals(a.ns(), b.ns());
		if (a.hasBs() || b.hasBs())
			return Objects.equals(a.bs(), b.bs());
		return a.equals(b);
	}

	public static Map<String, Object> fromAttrMap(Map<String, AttributeValue> attrs) {
		Map<String, Object> out = new LinkedHashMap<>();
		attrs.forEach((k, v) -> out.put(k, fromAttr(v)));
		return out;
	}

	public static Object fromAttr(AttributeValue a) {
		if (a.s() != null)
			return a.s();
		if (a.n() != null)
			return new BigDecimal(a.n());
		if (a.bool() != null)
			return a.bool();
		if (a.hasM()) {
			return a.m().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> fromAttr(e.getValue())));
		}
		if (a.hasL()) {
			List<Object> list = new ArrayList<>();
			for (AttributeValue av : a.l())
				list.add(fromAttr(av));
			return list;
		}
		if (a.nul() != null && a.nul())
			return null;
		if (a.hasSs())
			return new HashSet<>(a.ss());
		if (a.hasNs())
			return new HashSet<>(a.ns());
		if (a.hasBs())
			return new HashSet<>(a.bs());
		return null;
	}

}
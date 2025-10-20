package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.dto.InsertionSummary;
import sg.edu.nus.iss.edgp.masterdata.management.utility.GeneralUtility;

@RequiredArgsConstructor
@Service
public class StagingDataService {

	private final DynamoDbClient dynamoDbClient;
	
	
	public InsertionSummary insertToStaging(
	        String stagingTableName,
	        List<LinkedHashMap<String, Object>> rows,
	        String organizationId,
	        String policyId,
	        String domainName,
	        String fileId,
	        String uploadedBy
	) {
	    Objects.requireNonNull(stagingTableName, "stagingTableName");
	    if (domainName == null || domainName.isBlank()) {
	        throw new IllegalArgumentException("domain_name is mandatory");
	    }

	    // Discover existing column names
	    Set<String> existingKeys = getExistingAttributeKeys(stagingTableName);
	    Map<String, String> existingByLower = new HashMap<>();
	    for (String k : existingKeys) {
	        existingByLower.put(k.toLowerCase(Locale.ROOT), k);
	    }

	    List<Map<String, Object>> top50Preview = new ArrayList<>(50);
	    int total = 0;

	    final int MAX_BATCH = 25;
	    List<WriteRequest> batch = new ArrayList<>(MAX_BATCH);

	    for (Map<String, Object> src : rows) {
	        Map<String, AttributeValue> item = new LinkedHashMap<>();

	        // ️Case-insensitive matching to existing keys
	        for (var e : src.entrySet()) {
	            String rawKey = safeKey(e.getKey());
	            if (rawKey.isEmpty() || "id".equalsIgnoreCase(rawKey)) continue;
	            Object val = e.getValue();
	            if (val == null) continue;

	            // If this key already exists in DynamoDB (any case), use the existing casing
	            String lower = rawKey.toLowerCase(Locale.ROOT);
	            String canonicalKey = existingByLower.getOrDefault(lower, rawKey);

	            AttributeValue av = toAttr(val);
	            if (av != null) item.put(canonicalKey, av);
	        }

	        //️ System fields (always canonical lowercase)
	        String id = UUID.randomUUID().toString();
	        putS(item, "id", id);
	        putS(item, "organization_id", trimOrEmpty(organizationId));
	        putS(item, "policy_id", trimOrEmpty(policyId));
	        putS(item, "domain_name", domainName.trim().toLowerCase());
	        putS(item, "file_id", trimOrEmpty(fileId));
	        putS(item, "uploaded_by", trimOrEmpty(uploadedBy));
	        putS(item, "uploaded_date", GeneralUtility.nowSgt());
	        putN(item, "is_processed", "0");
	        putN(item, "is_handled", "0");

	        if (top50Preview.size() < 50) top50Preview.add(toPlainMap(item));

	        batch.add(WriteRequest.builder()
	                .putRequest(PutRequest.builder().item(item).build())
	                .build());

	        if (batch.size() == MAX_BATCH) {
	            flushBatchWithRetry(dynamoDbClient, stagingTableName, batch);
	            batch.clear();
	        }
	        total++;
	    }

	    if (!batch.isEmpty()) flushBatchWithRetry(dynamoDbClient, stagingTableName, batch);

	    return new InsertionSummary(total, top50Preview);
	}

	private Set<String> getExistingAttributeKeys(String tableName) {
	    Set<String> keys = new HashSet<>();
	    try {
	        ScanRequest scanReq = ScanRequest.builder()
	                .tableName(tableName)
	                .limit(1)
	                .build();
	        ScanResponse res = dynamoDbClient.scan(scanReq);
	        if (!res.items().isEmpty()) {
	            keys.addAll(res.items().get(0).keySet());
	        }
	    } catch (Exception e) {
	        // fallback to empty (new table)
	    }
	    return keys;
	}


	/**
	 * Collapse a row's keys case-insensitively.
	 * - Canonical key = lowercase (consistent for new items)
	 * - If multiple keys differ only by case (Email/email), LAST non-null wins (change if you prefer first).
	 * - Skips nulls (does not create the attribute).
	 * - Skips source "id" (the caller will generate a new one).
	 */
	private Map<String, Object> canonicalizeRowKeysCaseInsensitive(Map<String, Object> row) {
	    Map<String, Object> out = new LinkedHashMap<>();
	    for (Map.Entry<String, Object> e : row.entrySet()) {
	        String rawKey = e.getKey();
	        if (rawKey == null) continue;
	        if ("id".equalsIgnoreCase(rawKey)) continue; // we always generate our own id

	        String lower = rawKey.toLowerCase(Locale.ROOT);
	        Object val = e.getValue();

	        // LAST non-null wins; switch to "if (!out.containsKey(lower))" for FIRST non-null wins
	        if (val != null || !out.containsKey(lower)) {
	            out.put(lower, val);
	        }
	    }
	    return out;
	}

	
	private static String trimOrEmpty(String s) { return (s == null) ? "" : s.trim(); }

	private static void putS(Map<String, AttributeValue> item, String key, String val) {
	    if (val == null || val.isEmpty()) return;
	    item.put(key, AttributeValue.builder().s(val).build());
	}

	private static void putN(Map<String, AttributeValue> item, String key, String n) {
	    if (n == null || n.isBlank()) return;
	    item.put(key, AttributeValue.builder().n(n).build());
	}

	private static String safeKey(String k) {
	    if (k == null) return "";
	    return k.trim().replace(' ', '_').replaceAll("[^A-Za-z0-9_:-]", "_");
	}

	private AttributeValue toAttr(Object v) {
	    if (v == null) return null;

	    if (v instanceof String s) {
	        if (s.isEmpty()) return null;
	        
	        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
	            return AttributeValue.builder().bool(Boolean.parseBoolean(s)).build();
	        }
	       
	        try {
	            if (s.matches("^[+-]?\\d*(\\.\\d+)?([eE][+-]?\\d+)?$")) {
	                new BigDecimal(s); // validate
	                return AttributeValue.builder().n(s).build();
	            }
	        } catch (NumberFormatException ignore) {}
	        
	        return AttributeValue.builder().s(s).build();
	    } else if (v instanceof Integer i) {
	        return AttributeValue.builder().n(i.toString()).build();
	    } else if (v instanceof Long l) {
	        return AttributeValue.builder().n(l.toString()).build();
	    } else if (v instanceof Double d) {
	        return AttributeValue.builder().n(BigDecimal.valueOf(d).toPlainString()).build();
	    } else if (v instanceof BigDecimal bd) {
	        return AttributeValue.builder().n(bd.toPlainString()).build();
	    } else if (v instanceof Boolean b) {
	        return AttributeValue.builder().bool(b).build();
	    } else if (v instanceof List<?> list) {
	        List<AttributeValue> l = list.stream()
	                .map(this::toAttr)
	                .filter(Objects::nonNull)
	                .toList();
	        return AttributeValue.builder().l(l).build();
	    } else if (v instanceof Map<?, ?> m) {
	        Map<String, AttributeValue> inner = new LinkedHashMap<>();
	        for (var e : m.entrySet()) {
	            if (e.getKey() != null && e.getValue() != null) {
	                inner.put(e.getKey().toString(), toAttr(e.getValue()));
	            }
	        }
	        return AttributeValue.builder().m(inner).build();
	    }

	    // fallback
	    return AttributeValue.builder().s(v.toString()).build();
	}

	private void flushBatchWithRetry(DynamoDbClient ddb, String table, List<WriteRequest> wrs) {
	    Map<String, List<WriteRequest>> req = new HashMap<>();
	    req.put(table, new ArrayList<>(wrs));

	    BatchWriteItemResponse resp = ddb.batchWriteItem(
	            BatchWriteItemRequest.builder().requestItems(req).build()
	    );
	    Map<String, List<WriteRequest>> unprocessed = resp.unprocessedItems();

	    int attempt = 0;
	    while (unprocessed != null && !unprocessed.isEmpty()) {
	        int base = (int) Math.min(1000L * (1L << Math.min(attempt, 3)), 8000L);
	        int sleep = ThreadLocalRandom.current().nextInt(base / 2, base);
	        try { Thread.sleep(sleep); } catch (InterruptedException ignored) {}

	        // rebuild request each retry
	        BatchWriteItemRequest retryReq = BatchWriteItemRequest.builder()
	                .requestItems(unprocessed)
	                .build();
	        resp = ddb.batchWriteItem(retryReq);

	        unprocessed = resp.unprocessedItems();
	        attempt++;
	    }
	}
	
	private Map<String, Object> toPlainMap(Map<String, AttributeValue> item) {
        Map<String, Object> m = new LinkedHashMap<>();
        item.forEach((k, v) -> {
            if (v.s() != null) m.put(k, v.s());
            else if (v.n() != null) m.put(k, new BigDecimal(v.n()));
            else if (v.bool() != null) m.put(k, v.bool());
            else if (v.hasL()) m.put(k, v.l().stream().map(this::fromAttr).toList());
            else if (v.hasM()) {
                Map<String, Object> inner = new LinkedHashMap<>();
                v.m().forEach((ik, iv) -> inner.put(ik, fromAttr(iv)));
                m.put(k, inner);
            } else if (Boolean.TRUE.equals(v.nul())) m.put(k, null);
            else m.put(k, null);
        });
        return m;
    }


	private Object fromAttr(AttributeValue v) {
        if (v == null) return null;
        if (v.s() != null) return v.s();
        if (v.n() != null) return v.n();
        if (v.bool() != null) return v.bool();
        if (v.hasL()) return v.l().stream().map(this::fromAttr).toList();
        if (v.hasM()) {
            Map<String, Object> m = new LinkedHashMap<>();
            v.m().forEach((k, val) -> m.put(k, fromAttr(val)));
            return m;
        }
        if (v.nul() != null && v.nul()) return null;
        return null;
    }


}

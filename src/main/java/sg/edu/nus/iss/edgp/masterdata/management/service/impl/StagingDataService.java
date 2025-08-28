package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
            List<Map<String, String>> csvRows,
            String organizationId,
            String policyId,
            String domainName,
            String fileId,
            String uploadedBy
    ) {

        List<Map<String, Object>> top50Preview = new ArrayList<>(50);
        int total = 0;

        final int MAX_BATCH = 25;
        List<WriteRequest> batch = new ArrayList<>(MAX_BATCH);
       
        for (Map<String, String> src : csvRows) {
            Map<String, AttributeValue> item = new LinkedHashMap<>();
 
            for (var e : src.entrySet()) {
                var av = toAttributeValue(e.getValue());
                if (av != null) item.put(e.getKey().trim(), av);
            }
            String id = java.util.UUID.randomUUID().toString();
            item.put("id", avS(id));
            item.put("organization_id", avS(organizationId.trim()));
            item.put("policy_id", avS(policyId.trim()));
            item.put("domain_name", avS(domainName.trim()));
            item.put("file_id", avS(fileId));
            item.put("uploaded_by", avS(uploadedBy));
            item.put("uploaded_date", avS(GeneralUtility.nowSgt()));
            item.put("is_processed", avN("0"));
            item.put("is_handled", avN("0"));

           
            if (top50Preview.size() < 50) {
                top50Preview.add(toPlainMap(item));
            }

            batch.add(WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());

            if (batch.size() == MAX_BATCH) {
                flushBatch(dynamoDbClient, stagingTableName, batch);
                batch.clear();
            }
            total++;
        }
        if (!batch.isEmpty()) flushBatch(dynamoDbClient, stagingTableName, batch);

        return new InsertionSummary(total, top50Preview);
    }

 
    private static AttributeValue avS(String s) {
        return AttributeValue.builder().s(s).build();
    }
    private static AttributeValue avN(String n) {
        return AttributeValue.builder().n(n).build();
    }

    private static AttributeValue toAttributeValue(String raw) {
        if (raw == null) return AttributeValue.builder().nul(true).build();
        String s = raw.trim();
        if (s.isEmpty()) return AttributeValue.builder().nul(true).build();

        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return AttributeValue.builder().bool(Boolean.parseBoolean(s)).build();
        }
        if (s.matches("^-?\\d+$") || s.matches("^-?\\d*\\.\\d+$") || s.matches("^-?\\d+(?:[eE]-?\\d+)$")) {
            try { new java.math.BigDecimal(s); return avN(s); } catch (NumberFormatException ignore) {}
        }
        
        String iso = tryIsoNormalize(s);
        if (iso != null) return avS(iso);

        return avS(s);
    }

    private static String tryIsoNormalize(String s) {
        try { return java.time.Instant.parse(s).toString(); } catch (Exception ignored) {}
        try { return java.time.OffsetDateTime.parse(s).toInstant().toString(); } catch (Exception ignored) {}
        try { return java.time.ZonedDateTime.parse(s).toInstant().toString(); } catch (Exception ignored) {}
        try { return java.time.LocalDateTime.parse(s).toString(); } catch (Exception ignored) {}
        try { return java.time.LocalDate.parse(s).toString(); } catch (Exception ignored) {}
        return null;
    }

    
    private static Map<String, Object> toPlainMap(Map<String, AttributeValue> item) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (var e : item.entrySet()) {
            var v = e.getValue();
            if (v.s() != null) m.put(e.getKey(), v.s());
            else if (v.n() != null) m.put(e.getKey(), new java.math.BigDecimal(v.n()));
            else if (v.bool() != null) m.put(e.getKey(), v.bool());
            else if (Boolean.TRUE.equals(v.nul())) m.put(e.getKey(), null);
            else m.put(e.getKey(), null);
        }
        return m;
    }

    private static void flushBatch(
            DynamoDbClient ddb,
            String table,
            List<WriteRequest> wrs
    ) {
        Map<String, List<WriteRequest>> req = new HashMap<>();
        req.put(table, new ArrayList<>(wrs));
        int attempt = 0;
        while (true) {
            var resp = ddb.batchWriteItem(b -> b.requestItems(req));
            var unprocessed = resp.unprocessedItems().getOrDefault(table, List.of());
            if (unprocessed.isEmpty()) break;
            attempt++;
            int base = (int)Math.min(1000L * (1L << attempt), 8000L);
            int sleep = java.util.concurrent.ThreadLocalRandom.current().nextInt(base/2, base);
            try { Thread.sleep(sleep); } catch (InterruptedException ignored) {}
            req.put(table, unprocessed);
        }
    }

}

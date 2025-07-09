package sg.edu.nus.iss.edgp.masterdata.management.repository;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;

@Repository
public class MetadataRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public boolean tableExists(String schema, String tableName) {
		String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
		Integer count = jdbcTemplate.queryForObject(query, Integer.class, schema, tableName);
		return count != null && count > 0;
	}

	public void insertRow(String tableName, Map<String, String> rowData) throws SQLException {

		if (rowData == null || rowData.isEmpty()) {
			throw new IllegalArgumentException("No dynamic data provided for insert.");
		}

		Set<String> insertColumns = rowData.keySet();
		validateInsertColumns(tableName, insertColumns, jdbcTemplate);

		// Normalize and convert data types
		Map<String, Object> insertData = normalizeInsertData(tableName, rowData, insertColumns);

		String columns = insertData.keySet().stream().map(col -> "`" + col + "`").collect(Collectors.joining(", "));

		String placeholders = insertData.keySet().stream().map(col -> "?").collect(Collectors.joining(", "));

		String sql = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + placeholders + ")";
		System.out.println("SQL: " + sql);
		System.out.println("Values: " + Arrays.toString(insertData.values().toArray()));
		System.out.println("Column count: " + insertData.keySet().size());

		jdbcTemplate.update(sql, insertData.values().toArray());
	}

	public void validateInsertColumns(String tableName, Set<String> insertColumns, JdbcTemplate jdbcTemplate) {
		// 1. Get actual columns from the database table
		Set<String> dbColumns = jdbcTemplate.query("SELECT * FROM `" + tableName + "` LIMIT 1", rs -> {
			ResultSetMetaData meta = rs.getMetaData();
			Set<String> columns = new HashSet<>();
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				columns.add(meta.getColumnName(i).toLowerCase());
			}
			return columns;
		});

		// 2. Filter out system-managed or backend-only columns
		Set<String> excluded = Set.of("created_date", "updated_date");
		dbColumns.removeAll(excluded);

		// 3. Normalize insert columns
		Set<String> cleanedColumns = insertColumns.stream().map(col -> col == null ? "" : col.trim().toLowerCase())
				.collect(Collectors.toSet());

		// 4. Check if any column in the insert list is not in the DB table
		Set<String> missingColumns = new HashSet<>(cleanedColumns);
		missingColumns.removeAll(dbColumns);

		if (!missingColumns.isEmpty()) {
			throw new IllegalArgumentException(
					"Upload failed: Invalid column names found in your file: " + missingColumns);
		}
	}

	public Map<String, Object> normalizeInsertData(String tableName, Map<String, String> rawData,
			Set<String> numericColumns) throws SQLException {

		Map<String, Integer> columnTypeMap = getColumnTypes(tableName);
		return rawData.entrySet().stream().filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty())
				.collect(Collector.of(LinkedHashMap::new, (map, e) -> {
					String col = e.getKey().trim();
					String val = e.getValue();
					Integer sqlType = columnTypeMap.get(col.toLowerCase());

					Object finalValue;
					if (sqlType == null) {
						finalValue = val == null ? "" : val.trim();
					} else if (val == null || val.trim().isEmpty()) {
						finalValue = (sqlType == Types.INTEGER || sqlType == Types.DECIMAL || sqlType == Types.NUMERIC
								|| sqlType == Types.DOUBLE || sqlType == Types.FLOAT || sqlType == Types.DATE
								|| sqlType == Types.TIMESTAMP) ? null : "";
					} else {
						try {
							if (sqlType == Types.INTEGER)
								finalValue = Integer.parseInt(val.trim());
							else if (sqlType == Types.DECIMAL || sqlType == Types.NUMERIC)
								finalValue = new BigDecimal(val.trim());
							else if (sqlType == Types.DOUBLE || sqlType == Types.FLOAT)
								finalValue = Double.parseDouble(val.trim());
							else if (sqlType == Types.DATE)
								finalValue = Date.valueOf(val.trim());
							else if (sqlType == Types.TIMESTAMP)
								finalValue = Timestamp.valueOf(val.trim());
							else
								finalValue = val.trim();
						} catch (Exception ex) {
							finalValue = val.trim(); // fallback
						}
					}

					map.put(col, finalValue);
				}, (map1, map2) -> {
					map1.putAll(map2);
					return map1;
				}));

	}

	public Map<String, Integer> getColumnTypes(String tableName) throws SQLException {
		Map<String, Integer> columnTypes = new HashMap<>();

		try (Connection connection = jdbcTemplate.getDataSource().getConnection();
				PreparedStatement stmt = connection.prepareStatement("SELECT * FROM `" + tableName + "` LIMIT 1");
				ResultSet rs = stmt.executeQuery()) {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String columnName = metaData.getColumnName(i).toLowerCase(); // normalize
				int columnType = metaData.getColumnType(i); // java.sql.Types
				columnTypes.put(columnName, columnType);
			}
		}

		return columnTypes;
	}

	public List<Map<String, Object>> getDataByPolicyId(String table, SearchRequest searchReq) {
		String sql = "SELECT * FROM `" + table + "` WHERE policy_id = ? LIMIT ? OFFSET ?";
		return jdbcTemplate.queryForList(sql, searchReq.getPolicyId(), searchReq.getSize(),
				searchReq.getPage() * searchReq.getSize());
	}

	public List<Map<String, Object>> getDataByOrgId(String table, SearchRequest searchReq) {
		String sql = "SELECT * FROM `" + table + "` WHERE organization_id = ? LIMIT ? OFFSET ?";
		return jdbcTemplate.queryForList(sql, searchReq.getOrganizationId(), searchReq.getSize(),
				searchReq.getPage() * searchReq.getSize());
	}

	public List<Map<String, Object>> getDataByPolicyAndOrgId(String tableName, SearchRequest searchReq) {
		String sql = "SELECT * FROM `" + tableName + "` WHERE policy_id = ? and organization_id=? LIMIT ? OFFSET ?";
		return jdbcTemplate.queryForList(sql, searchReq.getPolicyId(), searchReq.getOrganizationId(),
				searchReq.getSize(), searchReq.getPage() * searchReq.getSize());
	}

	public List<Map<String, Object>> getAllData(String tableName, SearchRequest searchReq) {
		String sql = "SELECT * FROM `" + tableName + "` LIMIT ? OFFSET ?";
		return jdbcTemplate.queryForList(sql, searchReq.getSize(), searchReq.getPage() * searchReq.getSize());
	}
}

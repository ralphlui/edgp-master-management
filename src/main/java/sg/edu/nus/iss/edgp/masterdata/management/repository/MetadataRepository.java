package sg.edu.nus.iss.edgp.masterdata.management.repository;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MetadataRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public boolean tableExists(String schema, String tableName) {
		String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
		Integer count = jdbcTemplate.queryForObject(query, Integer.class, schema, tableName);
		return count != null && count > 0;
	}

	public void insertRowO(String tableName, Map<String, String> rowData) {
		if (rowData == null || rowData.isEmpty()) {
			throw new IllegalArgumentException("No data provided for insert.");
		}

		String columns = rowData.keySet().stream().map(col -> "`" + col + "`").collect(Collectors.joining(", "));

		String placeholders = rowData.keySet().stream().map(col -> "?").collect(Collectors.joining(", "));

		String sql = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + placeholders + ")";
		System.out.println("SQL: " + sql);
		System.out.println("Values: " + rowData.values());

		jdbcTemplate.update(sql, rowData.values().toArray());
	}

	public void insertRow(String tableName, Map<String, String> rowData) throws SQLException {

		if (rowData == null || rowData.isEmpty()) {
			throw new IllegalArgumentException("No dynamic data provided for insert.");
		}

		String columns = rowData.keySet().stream().map(col -> "`" + col + "`").collect(Collectors.joining(", "));

		String placeholders = rowData.keySet().stream().map(col -> "?").collect(Collectors.joining(", "));

		String sql = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + placeholders + ")";
		System.out.println("SQL: " + sql);
		System.out.println("Values: " + Arrays.toString(rowData.values().toArray()));
		System.out.println("Column count: " + rowData.keySet().size());

		jdbcTemplate.update(sql, rowData.values().toArray());
	}

}

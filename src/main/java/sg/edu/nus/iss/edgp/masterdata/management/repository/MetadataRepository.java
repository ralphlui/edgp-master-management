package sg.edu.nus.iss.edgp.masterdata.management.repository;

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

}

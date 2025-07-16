package sg.edu.nus.iss.edgp.masterdata.management.repository;

import java.util.List;
import java.util.Map; 

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import sg.edu.nus.iss.edgp.masterdata.management.dto.SearchRequest;

@Repository
public class MetadataRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	

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

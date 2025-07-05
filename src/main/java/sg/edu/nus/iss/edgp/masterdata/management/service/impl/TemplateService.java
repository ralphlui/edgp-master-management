package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.TemplateFileFormat;
import sg.edu.nus.iss.edgp.masterdata.management.repository.MetadataRepository;
import sg.edu.nus.iss.edgp.masterdata.management.service.ITemplateService;

@RequiredArgsConstructor
@Service
public class TemplateService implements ITemplateService {
	
	private static final Logger logger = LoggerFactory.getLogger(TemplateService.class);

	
	@Autowired
    private JdbcTemplate jdbcTemplate;
	
	private final MetadataRepository metadataRepository;

	@Override
	public void createTableFromCsvTemplate(MultipartFile file,String tableName) {
		 List<TemplateFileFormat> fields = parseCsvTemplate(file);
	       // String tableName = generateTableName(file.getOriginalFilename());
	        String createSql = buildCreateTableSQL(tableName.toLowerCase(), fields);
	        jdbcTemplate.execute(createSql);
		
	}

	@Override
	public List<TemplateFileFormat> parseCsvTemplate(MultipartFile file) {
		List<TemplateFileFormat> fields = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            boolean skipHeader = true;
            while ((line = reader.readLine()) != null) {
                if (skipHeader) {
                    skipHeader = false;
                    continue;
                }
                String[] parts = line.split(",");
                System.out.println(line);
                if (parts.length < 4) continue;

                String fieldName = parts[0].trim().replaceAll("[^a-zA-Z0-9_]", "_");
                String description = parts[1].trim();
                String dataType = parts[2].trim().toUpperCase();
                int length = Integer.parseInt(parts[3].trim());

                fields.add(new TemplateFileFormat(fieldName,description, dataType, length));
            }
        } catch (IOException e) {
			
			logger.error("parseCsvTemplate exception... {}", e.toString());
		}
        return fields;
	}
	
	private String generateTableName(String filename) {
        return "mdm_" + filename.toLowerCase()
                .replace(".csv", "")
                .replaceAll("[^a-z0-9_]", "_");
    }
	
	public boolean checkTableIfExists(String tableName) throws SQLException {
	    String schema = jdbcTemplate.getDataSource().getConnection().getCatalog(); // get current DB/schema
	    return metadataRepository.tableExists(schema, tableName);
	}
 

    private String buildCreateTableSQL(String tableName, List<TemplateFileFormat> fields) {
        String columnDefs = fields.stream()
                .map(f -> "`" + f.getFieldName() + "` " + mapDataType(f.getDataType(), f.getLength()))
                .collect(Collectors.joining(", "));
        
          
        String staticColumns = String.join(", ",
            "`id` INT AUTO_INCREMENT PRIMARY KEY",
            "`organization_id` VARCHAR(255) NOT NULL",
            "`policy_id` VARCHAR(255) NOT NULL",
            "`created_by` VARCHAR(100)",
            "`created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "`updated_by` VARCHAR(100)",
            "`updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        );
        
        return "CREATE TABLE IF NOT EXISTS `"  + tableName + "` (" + staticColumns + ", " + columnDefs + ")";
    }
        
     
    

    private String mapDataType(String type, int length) {
        switch (type.toUpperCase()) {
            case "CLNT":
            case "CHAR":
            case "LANG":
            case "STRING":
                return "VARCHAR(" + length + ")";
            case "DATS":
                return "DATE";
            case "NUMC":
                return "INT";
            case "INT":
            case "INTEGER":
                return "INT";
            case "DEC":
            case "DECIMAL":
                return "DECIMAL(15,2)";
            default:
                return "VARCHAR(" + length + ")";
        }
    }


}

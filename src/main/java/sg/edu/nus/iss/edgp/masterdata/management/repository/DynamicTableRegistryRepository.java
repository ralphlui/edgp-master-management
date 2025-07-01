package sg.edu.nus.iss.edgp.masterdata.management.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import sg.edu.nus.iss.edgp.masterdata.management.entity.DynamicTableRegistry;
 

@Repository
public interface DynamicTableRegistryRepository  extends JpaRepository<DynamicTableRegistry, String> {
	
	List<DynamicTableRegistry> findAll();


}

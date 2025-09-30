package sg.edu.nus.iss.edgp.masterdata.management.service;

import java.util.List;


public interface IDomainService {

	List<String> findDomains();
	
	boolean createDomain(String domainName);
	
}

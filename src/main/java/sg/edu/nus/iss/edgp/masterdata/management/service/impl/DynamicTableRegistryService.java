package sg.edu.nus.iss.edgp.masterdata.management.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.entity.DynamicTableRegistry;
import sg.edu.nus.iss.edgp.masterdata.management.exception.MasterdataServiceException;
import sg.edu.nus.iss.edgp.masterdata.management.repository.DynamicTableRegistryRepository;
import sg.edu.nus.iss.edgp.masterdata.management.service.IDynamicTableRegistryService;

@RequiredArgsConstructor
@Service
public class DynamicTableRegistryService implements IDynamicTableRegistryService{

	private static final Logger logger = LoggerFactory.getLogger(DynamicTableRegistryService.class);

	private final DynamicTableRegistryRepository categoryRepository;
	 
	@Override
	public List<String> findCategories() {
		
		List<String> retList = new ArrayList<String>();
	try {	
		List<DynamicTableRegistry> categories = categoryRepository.findAll();
		logger.info("Total record in findCategories " + categories.size());
		
		if(categories !=null) {
			retList = categories.stream()
                    .map(DynamicTableRegistry::getName)
                    .collect(Collectors.toList());
		}

		return retList;
	
	} catch (Exception e) {
		logger.error("findCategories exception... {}", e.toString());
		throw new MasterdataServiceException("An error occured while findCategories", e);
	
	}
	}
}

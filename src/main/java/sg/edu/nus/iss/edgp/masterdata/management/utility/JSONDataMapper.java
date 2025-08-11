package sg.edu.nus.iss.edgp.masterdata.management.utility;

import java.util.Objects;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.masterdata.management.pojo.PolicyRoot;

@Component
@RequiredArgsConstructor
public class JSONDataMapper {
	
	public PolicyRoot getPolicyAndRules(String response) {
		PolicyRoot policyRoot = new PolicyRoot();
		try {
			if(!Objects.equals(response,"")) {
				ObjectMapper mapper = new ObjectMapper();
				policyRoot = mapper.readValue(response, PolicyRoot.class);
			}
		}catch(JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		return policyRoot;
	}
	
}

package sg.edu.nus.iss.edgp.masterdata.management.service;

import sg.edu.nus.iss.edgp.masterdata.management.dto.AuditDTO;

public interface IAuditService {
	void sendMessage(AuditDTO autAuditDTO,String token);
 
}

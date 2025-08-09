package sg.edu.nus.iss.edgp.masterdata.management.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Metadata {
    private String dataType = "tabular";
    private String domainName;
    private String fileId;
    private String policyId;
}
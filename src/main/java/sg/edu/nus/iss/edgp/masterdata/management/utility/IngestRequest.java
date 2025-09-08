package sg.edu.nus.iss.edgp.masterdata.management.utility;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Map;

public record IngestRequest(
    @NotBlank
    String domainName,

    @NotBlank
    String policyId,


    @NotNull
    @Size(min = 1)
    List<@Valid Item> items
) {
    public record Item(
        @NotNull
        @NotEmpty
        Map<String, Object> attributes
    ) {}
}

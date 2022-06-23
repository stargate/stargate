package io.stargate.sgv2.docsapi.api.v2.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record MultiDocsResponse(
    @Schema(
            description = "The ids of the documents.",
            example =
                "[822dc277-9121-4791-8b01-da8154e67d5d, 6334dft4-9153-3642-4f32-da8154e67d5d]")
        List<String> documentIds,

    // execution profile
    @Schema(
            description =
                "Profiling information related to the execution of the request. Only set if the endpoint supports profiling and `profile` query parameter is set to `true`.",
            nullable = true,
            example =
                """
                  {
                     "description":"root",
                     "queries":[],
                     "nested":[
                        {
                           "description":"FILTER: speed LT 1000",
                           "queries":[
                              {
                                 "cql":"SELECT key, leaf, WRITETIME(leaf) FROM cycling.events WHERE p0 = ? AND leaf = ? AND p1 = ? AND dbl_value < ? ALLOW FILTERING",
                                 "executionCount":1,
                                 "rowCount":1
                              }
                           ],
                           "nested":[]
                        },
                        {
                           "description":"LoadProperties",
                           "queries":[
                              {
                                 "cql":"SELECT key, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45, p46, p47, p48, p49, p50, p51, p52, p53, p54, p55, p56, p57, p58, p59, p60, p61, p62, p63, leaf, text_value, dbl_value, bool_value, WRITETIME(leaf) FROM cycling.events WHERE key = ?",
                                 "executionCount":1,
                                 "rowCount":21
                              }
                           ],
                           "nested":[]
                        }
                     ]
                  }
                  """)
        ExecutionProfile profile) {}

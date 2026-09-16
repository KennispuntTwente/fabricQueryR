# KQL recovery evidence

`integration-fabric-kql-graphql-recovery` creates temporary tables and OneLake sources.
It verifies a valid and a missing source independently, including the successful
row and the failed source ID. A second case performs real upload, ingestion,
status, query and cleanup recovery calls, injecting only the initial cleanup
failure. This establishes client recovery; it does not establish a real storage
permission denial.

The queued ingestion API used by this package has no client cancellation method.
To verify a service-canceled operation, supply a recent
`FABRIC_TEST_KQL_CANCELED_INGESTION_ID` and its
`FABRIC_TEST_KQL_CANCELED_TABLE`, then set
`FABRIC_TEST_REQUIRED_FEATURES=kql-cancellation`. The distinct cancellation test
requires a terminal canceled/partially canceled result and per-source details.
An ordinary run reports this case as skipped when that service fixture is absent.

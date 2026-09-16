The external shortcut test reads `FABRIC_TEST_EXTERNAL_SHORTCUT_JSON`. Use
`external-shortcut.example.json` as its structure. Provision a Fabric cloud
connection that the integration identity can use, and put the small UTF-8
`basic.csv` fixture at that connection's source location with exactly the bytes
in `expectedText`. Store the JSON as the corresponding GitHub environment secret
or set it in the local R session. Do not put source credentials in this JSON;
the Fabric connection manages them.

The test creates a unique shortcut in the marked test Lakehouse, checks its
target and the independently specified file bytes, and deletes only that
shortcut. It never writes to the external source. Other supported external
target types can use the same test by replacing `target` with their documented
REST target object. Without configuration, external coverage is reported as
skipped. The CSV-to-Delta transformation test uses a temporary OneLake source
and runs without an external connection when `FABRIC_TEST_SHORTCUT_TRANSFORMS=true`.

On 2026-09-15 the persistent workspace rejected the documented `csvToDelta`
request with `RequestBodyValidationFailed: Custom properties not found in the
request.` This was reproduced after checking the request against the current
Microsoft REST schema. The transformation test is therefore an explicit opt-in
and fails if enabled against that unsupported service contract; a skip is not
reported as successful transformation coverage. Enable the matching GitHub
environment variable when the tenant supports the endpoint.

Use `FABRIC_TEST_REQUIRED_FEATURES=shortcut-transforms,shortcut-cache` in a
designated supported/delegated lane. A missing transform opt-in or unsupported
cache-reset principal then fails, rather than skipping the required evidence.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_ipc::writer::FileWriter;
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch,
    StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use deltalake::DeltaTableBuilder;
use extendr_api::prelude::*;
use url::Url;

const TABLE_NAME: &str = "__fabricqueryr_delta_table";

fn optional_whole_number(value: f64, label: &str) -> Result<Option<u64>> {
    if value == -1.0 {
        return Ok(None);
    }
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
        bail!("{label} must be -1 or a non-negative whole number");
    }
    if value > 9_007_199_254_740_992.0 {
        bail!("{label} must not exceed 2^53");
    }
    Ok(Some(value as u64))
}

fn redact_error(error: &anyhow::Error, bearer_token: &str) -> String {
    let mut message = format!("{error:#}");
    if !bearer_token.is_empty() {
        message = message.replace(bearer_token, "<redacted>");
    }
    message
}

fn exact_decimal_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) | DataType::Utf8View => {
            DataType::Utf8
        }
        DataType::BinaryView => DataType::Binary,
        DataType::Struct(fields) => DataType::Struct(exact_decimal_fields(fields)),
        DataType::List(field) => DataType::List(exact_decimal_field(field)),
        DataType::LargeList(field) => DataType::LargeList(exact_decimal_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(exact_decimal_field(field), *size)
        }
        DataType::Map(field, ordered) => DataType::Map(exact_decimal_field(field), *ordered),
        _ => data_type.clone(),
    }
}

fn exact_decimal_field(field: &FieldRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(exact_decimal_data_type(field.data_type())),
    )
}

fn exact_decimal_fields(fields: &Fields) -> Fields {
    fields
        .iter()
        .map(exact_decimal_field)
        .collect::<Vec<_>>()
        .into()
}

fn exact_decimal_list<O: OffsetSizeTrait>(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    let list = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .context("Delta list column had an unexpected Arrow representation")?;
    let values = exact_decimal_array(list.values())?;
    let field = Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(values.data_type().clone()),
    );
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )?))
}

fn exact_decimal_array(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            Ok(cast(array, &DataType::Utf8).context("formatting an exact Delta decimal")?)
        }
        DataType::Utf8View => {
            Ok(cast(array, &DataType::Utf8).context("normalizing a Delta string view")?)
        }
        DataType::BinaryView => {
            Ok(cast(array, &DataType::Binary).context("normalizing a Delta binary view")?)
        }
        DataType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .context("Delta struct column had an unexpected Arrow representation")?;
            let columns = values
                .columns()
                .iter()
                .map(exact_decimal_array)
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                exact_decimal_fields(fields),
                columns,
                values.nulls().cloned(),
                values.len(),
            )?))
        }
        DataType::List(field) => exact_decimal_list::<i32>(array, field),
        DataType::LargeList(field) => exact_decimal_list::<i64>(array, field),
        DataType::FixedSizeList(field, size) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .context("Delta fixed-size list had an unexpected Arrow representation")?;
            let child = exact_decimal_array(values.values())?;
            let field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(child.data_type().clone()),
            );
            Ok(Arc::new(FixedSizeListArray::try_new(
                field,
                *size,
                child,
                values.nulls().cloned(),
            )?))
        }
        DataType::Map(field, ordered) => {
            let values = array
                .as_any()
                .downcast_ref::<MapArray>()
                .context("Delta map column had an unexpected Arrow representation")?;
            let entries: ArrayRef = Arc::new(values.entries().clone());
            let entries = exact_decimal_array(&entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .context("normalized Delta map entries were not an Arrow struct")?
                .clone();
            let field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(entries.data_type().clone()),
            );
            Ok(Arc::new(MapArray::try_new(
                field,
                values.offsets().clone(),
                entries,
                values.nulls().cloned(),
                *ordered,
            )?))
        }
        _ => Ok(array.clone()),
    }
}

fn exact_decimal_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        exact_decimal_fields(schema.fields()),
        schema.metadata().clone(),
    ))
}

fn exact_decimal_batch(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(exact_decimal_array)
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).context("normalizing exact Delta decimals")
}

async fn read_delta_to_ipc(
    uri: &str,
    bearer_token: &str,
    version: Option<u64>,
    columns: &[String],
    limit: Option<u64>,
    ipc_path: &str,
) -> Result<(u64, u64)> {
    deltalake::azure::register_handlers(None);

    let url = Url::parse(uri).with_context(|| format!("invalid Delta table URI: {uri}"))?;
    let mut builder = DeltaTableBuilder::from_url(url).context("creating Delta table builder")?;
    if uri.starts_with("abfs://") || uri.starts_with("abfss://") {
        if bearer_token.is_empty() {
            bail!("a bearer token is required for an ABFS or ABFSS table");
        }
        builder = builder.with_storage_options(HashMap::from([
            ("bearer_token".to_string(), bearer_token.to_string()),
            ("use_fabric_endpoint".to_string(), "true".to_string()),
        ]));
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    let table = builder
        .load()
        .await
        .context("loading Delta table snapshot")?;

    let ctx = SessionContext::new();
    table
        .update_datafusion_session(&ctx.state())
        .context("registering Delta object store with DataFusion")?;
    let provider = table
        .table_provider()
        .await
        .context("building Delta table provider")?;
    ctx.register_table(TABLE_NAME, provider)
        .context("registering Delta table provider")?;

    let mut frame = ctx
        .table(TABLE_NAME)
        .await
        .context("creating Delta DataFrame")?;
    if !columns.is_empty() {
        let selected = columns.iter().map(String::as_str).collect::<Vec<_>>();
        frame = frame
            .select_columns(&selected)
            .context("selecting Delta columns")?;
    }
    if let Some(limit) = limit {
        let limit = usize::try_from(limit).context("row limit exceeds usize")?;
        frame = frame.limit(0, Some(limit)).context("applying row limit")?;
    }

    let schema = exact_decimal_schema(&frame.schema().inner().clone());
    let batches = frame
        .collect()
        .await
        .context("executing Delta scan")?
        .iter()
        .map(|batch| exact_decimal_batch(batch, schema.clone()))
        .collect::<Result<Vec<_>>>()?;
    let row_count = batches.iter().try_fold(0_u64, |total, batch| {
        total
            .checked_add(batch.num_rows() as u64)
            .context("row count overflow")
    })?;

    let file = File::create(ipc_path)
        .with_context(|| format!("creating Arrow IPC output at {ipc_path}"))?;
    let mut writer =
        FileWriter::try_new(file, schema.as_ref()).context("creating Arrow IPC writer")?;
    for batch in &batches {
        writer.write(batch).context("writing Arrow IPC batch")?;
    }
    writer.finish().context("finishing Arrow IPC output")?;

    let loaded_version = u64::try_from(table.version().unwrap_or_default())
        .context("loaded Delta version is negative")?;
    Ok((loaded_version, row_count))
}

/// Read a Delta table with delta-rs and write Arrow IPC output.
///
/// @keywords internal
/// @noRd
#[extendr]
fn fabric_delta_rs_read_to_ipc(
    uri: &str,
    bearer_token: &str,
    version: f64,
    columns: Vec<String>,
    limit: f64,
    ipc_path: &str,
) -> extendr_api::Result<List> {
    let parsed_version = optional_whole_number(version, "version")
        .map_err(|error| extendr_api::Error::Other(error.to_string()))?;
    let parsed_limit = optional_whole_number(limit, "limit")
        .map_err(|error| extendr_api::Error::Other(error.to_string()))?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| extendr_api::Error::Other(error.to_string()))?;
    match runtime.block_on(read_delta_to_ipc(
        uri,
        bearer_token,
        parsed_version,
        &columns,
        parsed_limit,
        ipc_path,
    )) {
        Ok((loaded_version, row_count)) => Ok(list!(
            version = loaded_version as f64,
            rows = row_count as f64,
            path = ipc_path
        )),
        Err(error) => Err(extendr_api::Error::Other(redact_error(
            &error,
            bearer_token,
        ))),
    }
}

extendr_module! {
    mod fabric_query_r;
    fn fabric_delta_rs_read_to_ipc;
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, StringArray};

    #[test]
    fn optional_numbers_accept_sentinel_and_whole_values() {
        assert_eq!(optional_whole_number(-1.0, "value").unwrap(), None);
        assert_eq!(optional_whole_number(0.0, "value").unwrap(), Some(0));
        assert_eq!(
            optional_whole_number(9_007_199_254_740_992.0, "value").unwrap(),
            Some(9_007_199_254_740_992)
        );
    }

    #[test]
    fn optional_numbers_reject_invalid_values() {
        for value in [-2.0, 1.5, f64::NAN, f64::INFINITY] {
            assert!(optional_whole_number(value, "value").is_err());
        }
    }

    #[test]
    fn errors_redact_bearer_tokens() {
        let error = anyhow::anyhow!("request failed with secret-token");
        let message = redact_error(&error, "secret-token");
        assert!(!message.contains("secret-token"));
        assert!(message.contains("<redacted>"));
    }

    #[test]
    fn decimals_are_formatted_without_losing_precision() {
        let decimal =
            Decimal128Array::from(vec![Some(12345678901234567890123456789012345678_i128)])
                .with_precision_and_scale(38, 2)
                .unwrap();
        let decimal: ArrayRef = Arc::new(decimal);
        let normalized = exact_decimal_array(&decimal).unwrap();
        let strings = normalized.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(strings.value(0), "123456789012345678901234567890123456.78");
    }
}

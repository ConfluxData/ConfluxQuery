//! Streaming result rendering with separate human and machine value policies.

use arrow_array::{Array, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_json::writer::{JsonArray, WriterBuilder as JsonWriterBuilder};
use arrow_schema::DataType;
use serde_json::Value;
use std::fmt;
use std::io::{self, Write};
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub struct DisplayOptions {
    pub decimal_places: usize,
    pub string_truncate: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Vertical,
    Csv,
    Tsv,
    Json,
    JsonLines,
}

impl FromStr for OutputFormat {
    type Err = OutputError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "vertical" => Ok(Self::Vertical),
            "csv" => Ok(Self::Csv),
            "tsv" => Ok(Self::Tsv),
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            _ => Err(OutputError::message(format!(
                "unknown output format '{value}'; expected table, vertical, csv, tsv, json, or jsonl"
            ))),
        }
    }
}

#[derive(Debug)]
pub struct OutputError {
    message: String,
    io_kind: Option<io::ErrorKind>,
}

impl OutputError {
    fn message(message: impl Into<String>) -> Self {
        let message = message.into();
        let io_kind = message
            .to_ascii_lowercase()
            .contains("broken pipe")
            .then_some(io::ErrorKind::BrokenPipe);
        Self { message, io_kind }
    }

    #[must_use]
    pub fn is_broken_pipe(&self) -> bool {
        self.io_kind == Some(io::ErrorKind::BrokenPipe)
    }
}

impl From<io::Error> for OutputError {
    fn from(error: io::Error) -> Self {
        Self {
            message: error.to_string(),
            io_kind: Some(error.kind()),
        }
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for OutputError {}

/// Incrementally writes batches without retaining prior batches.
pub struct StreamOutput<W: Write> {
    writer: W,
    format: OutputFormat,
    options: DisplayOptions,
    wrote_batch: bool,
    wrote_json_row: bool,
    rows: usize,
}

impl<W: Write> StreamOutput<W> {
    /// Start an incremental result stream.
    ///
    /// # Errors
    ///
    /// Returns an output error if the format requires an opening token that
    /// cannot be written.
    pub fn new(
        writer: W,
        format: OutputFormat,
        options: DisplayOptions,
    ) -> Result<Self, OutputError> {
        let mut output = Self {
            writer,
            format,
            options,
            wrote_batch: false,
            wrote_json_row: false,
            rows: 0,
        };
        if format == OutputFormat::Json {
            output.writer.write_all(b"[")?;
        }
        Ok(output)
    }

    /// Write one batch and release it before the next batch arrives.
    ///
    /// # Errors
    ///
    /// Returns an output error for unsupported values or failed writes.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), OutputError> {
        match self.format {
            OutputFormat::Table => self
                .writer
                .write_all(render_table(batch, self.options)?.as_bytes())?,
            OutputFormat::Vertical => self.write_vertical(batch)?,
            OutputFormat::Csv | OutputFormat::Tsv => self.write_delimited(batch)?,
            OutputFormat::Json | OutputFormat::JsonLines => self.write_json(batch)?,
        }
        self.wrote_batch = true;
        self.rows += batch.num_rows();
        Ok(())
    }

    /// Close the stream and return the number of rows written.
    ///
    /// # Errors
    ///
    /// Returns an output error if closing or flushing the stream fails.
    pub fn finish(mut self) -> Result<usize, OutputError> {
        if self.format == OutputFormat::Json {
            self.writer.write_all(b"]\n")?;
        }
        self.writer.flush()?;
        Ok(self.rows)
    }

    fn write_delimited(&mut self, batch: &RecordBatch) -> Result<(), OutputError> {
        let delimiter = if self.format == OutputFormat::Csv {
            b','
        } else {
            b'\t'
        };
        let mut writer = arrow_csv::WriterBuilder::new()
            .with_header(!self.wrote_batch)
            .with_delimiter(delimiter)
            .with_null("NULL".to_owned())
            .build(&mut self.writer);
        writer
            .write(batch)
            .map_err(|error| OutputError::message(error.to_string()))
    }

    fn write_json(&mut self, batch: &RecordBatch) -> Result<(), OutputError> {
        // Arrow handles nested values; one batch is then adjusted so decimals
        // are JSON strings and cannot lose precision in downstream clients.
        let mut encoded = Vec::new();
        {
            let mut writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut encoded);
            writer
                .write(batch)
                .map_err(|error| OutputError::message(error.to_string()))?;
            writer
                .finish()
                .map_err(|error| OutputError::message(error.to_string()))?;
        }
        let mut rows: Vec<Value> = serde_json::from_slice(&encoded)
            .map_err(|error| OutputError::message(error.to_string()))?;
        stringify_decimals(batch, &mut rows);
        for row in &rows {
            if self.format == OutputFormat::Json && self.wrote_json_row {
                self.writer.write_all(b",")?;
            }
            serde_json::to_writer(&mut self.writer, row)
                .map_err(|error| OutputError::message(error.to_string()))?;
            if self.format == OutputFormat::JsonLines {
                self.writer.write_all(b"\n")?;
            }
            self.wrote_json_row = true;
        }
        Ok(())
    }

    fn write_vertical(&mut self, batch: &RecordBatch) -> Result<(), OutputError> {
        for row in 0..batch.num_rows() {
            writeln!(
                self.writer,
                "*************************** {}. row ***************************",
                self.rows + row + 1
            )?;
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                writeln!(
                    self.writer,
                    "{}: {}",
                    field.name(),
                    render_value(column.as_ref(), row, self.options)?
                )?;
            }
        }
        Ok(())
    }
}

fn stringify_decimals(batch: &RecordBatch, rows: &mut [Value]) {
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        if let DataType::Decimal128(_, scale) = field.data_type() {
            let array = batch.column(column_index);
            let decimals = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("type checked");
            for (row_index, object) in rows.iter_mut().enumerate() {
                if !decimals.is_null(row_index) {
                    object[field.name()] =
                        Value::String(exact_decimal(decimals.value(row_index), *scale));
                }
            }
        }
    }
}

/// Render one Arrow batch as a human-oriented Unicode table.
///
/// # Errors
///
/// Returns an error when the human renderer does not support a column type.
pub fn render_table(batch: &RecordBatch, options: DisplayOptions) -> Result<String, OutputError> {
    let headers = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            values.push(render_value(column.as_ref(), row, options)?);
        }
        rows.push(values);
    }
    let widths = (0..headers.len())
        .map(|column| {
            rows.iter()
                .map(|row| row[column].chars().count())
                .max()
                .unwrap_or(0)
                .max(headers[column].chars().count())
        })
        .collect::<Vec<_>>();
    let border = |left: char, middle: char, right: char| {
        let body = widths
            .iter()
            .map(|width| "─".repeat(width + 2))
            .collect::<Vec<_>>()
            .join(&middle.to_string());
        format!("{left}{body}{right}\n")
    };
    let mut output = border('┌', '┬', '┐');
    output.push_str(&format_row(&headers, &widths));
    output.push_str(&border('├', '┼', '┤'));
    for row in &rows {
        output.push_str(&format_row(row, &widths));
    }
    output.push_str(&border('└', '┴', '┘'));
    Ok(output)
}

fn format_row(values: &[String], widths: &[usize]) -> String {
    let cells = values
        .iter()
        .zip(widths)
        .map(|(value, width)| format!(" {value:<width$} "))
        .collect::<Vec<_>>()
        .join("│");
    format!("│{cells}│\n")
}

fn render_value(
    array: &dyn Array,
    row: usize,
    options: DisplayOptions,
) -> Result<String, OutputError> {
    if array.is_null(row) {
        return Ok("NULL".into());
    }
    match array.data_type() {
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("type checked")
            .value(row)
            .to_string()),
        DataType::Utf8 => Ok(truncate(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("type checked")
                .value(row),
            options.string_truncate,
        )),
        DataType::Decimal128(_, scale) => Ok(decimal(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("type checked")
                .value(row),
            *scale,
            options.decimal_places,
        )),
        _ => ArrayFormatter::try_new(array, &FormatOptions::default())
            .map(|formatter| formatter.value(row).to_string())
            .map_err(|error| OutputError::message(error.to_string())),
    }
}

fn truncate(value: &str, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value.into();
    }
    if maximum == 0 {
        return String::new();
    }
    value
        .chars()
        .take(maximum.saturating_sub(1))
        .chain(std::iter::once('…'))
        .collect()
}

fn exact_decimal(value: i128, scale: i8) -> String {
    let negative = value < 0;
    let scale = usize::try_from(scale.max(0)).expect("non-negative scale");
    let digits = value.unsigned_abs().to_string();
    let padded = format!("{:0>width$}", digits, width = scale + 1);
    let split = padded.len() - scale;
    let (whole, fraction) = padded.split_at(split);
    let sign = if negative { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}

fn decimal(value: i128, scale: i8, maximum_places: usize) -> String {
    let negative = value < 0;
    let scale = usize::try_from(scale.max(0)).expect("non-negative scale");
    let shown_scale = scale.min(maximum_places);
    let removed = scale - shown_scale;
    let factor = 10_u128.pow(u32::try_from(removed).expect("Arrow decimal scale fits u32"));
    let absolute = value.unsigned_abs();
    let mut rounded = absolute / factor;
    let remainder = absolute % factor;
    if removed > 0 {
        let half = factor / 2;
        if remainder > half || (remainder == half && rounded % 2 == 1) {
            rounded += 1;
        }
    }
    let rounded = i128::try_from(rounded).expect("valid Arrow Decimal128 magnitude");
    let signed = if negative { -rounded } else { rounded };
    exact_decimal(
        signed,
        i8::try_from(shown_scale).expect("decimal scale fits i8"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, ListArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(18, 6), true),
        ]));
        let amount = Decimal128Array::from(vec![Some(123_456_789), None])
            .with_precision_and_scale(18, 6)
            .unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("雪だるま☃"), None])) as ArrayRef,
                Arc::new(amount),
            ],
        )
        .unwrap()
    }

    fn output(format: OutputFormat) -> String {
        let mut bytes = Vec::new();
        let mut stream = StreamOutput::new(
            &mut bytes,
            format,
            DisplayOptions {
                decimal_places: 3,
                string_truncate: 4,
            },
        )
        .unwrap();
        stream.write_batch(&sample()).unwrap();
        stream.finish().unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn machine_formats_preserve_exact_values_null_and_unicode() {
        assert_eq!(
            output(OutputFormat::Csv),
            "name,amount\n雪だるま☃,123.456789\nNULL,NULL\n"
        );
        assert_eq!(
            output(OutputFormat::Tsv),
            "name\tamount\n雪だるま☃\t123.456789\nNULL\tNULL\n"
        );
        assert_eq!(
            output(OutputFormat::Json),
            "[{\"name\":\"雪だるま☃\",\"amount\":\"123.456789\"},{\"name\":null,\"amount\":null}]\n"
        );
        assert_eq!(
            output(OutputFormat::JsonLines),
            "{\"name\":\"雪だるま☃\",\"amount\":\"123.456789\"}\n{\"name\":null,\"amount\":null}\n"
        );
    }

    #[test]
    fn json_supports_nested_values() {
        let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);
        let batch =
            RecordBatch::try_from_iter(vec![("items", Arc::new(list) as ArrayRef)]).unwrap();
        let mut bytes = Vec::new();
        let mut stream = StreamOutput::new(
            &mut bytes,
            OutputFormat::JsonLines,
            DisplayOptions {
                decimal_places: 3,
                string_truncate: 80,
            },
        )
        .unwrap();
        stream.write_batch(&batch).unwrap();
        stream.finish().unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "{\"items\":[1,2]}\n{\"items\":null}\n"
        );
    }

    #[test]
    fn display_transformations_do_not_mutate_source_values() {
        assert_eq!(truncate("abcdef", 4), "abc…");
        assert_eq!(decimal(123_456_789, 6, 3), "123.457");
        assert_eq!(decimal(123_456_500, 6, 3), "123.456");
        assert_eq!(decimal(123_455_500, 6, 3), "123.456");
    }

    #[test]
    fn broken_pipe_is_classified() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = StreamOutput::new(
            Broken,
            OutputFormat::Json,
            DisplayOptions {
                decimal_places: 3,
                string_truncate: 80,
            },
        )
        .err()
        .unwrap();
        assert!(error.is_broken_pipe());
    }

    #[test]
    fn table_formats_additional_arrow_types() {
        let batch = RecordBatch::try_from_iter(vec![(
            "items",
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let table = render_table(
            &batch,
            DisplayOptions {
                decimal_places: 3,
                string_truncate: 80,
            },
        )
        .unwrap();
        assert!(table.contains('1'));
    }
}

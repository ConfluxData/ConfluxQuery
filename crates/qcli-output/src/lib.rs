//! Human result rendering with display-only transformations.

use arrow_array::{Array, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub struct DisplayOptions {
    pub decimal_places: usize,
    pub string_truncate: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputError(String);

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OutputError {}

/// Render one Arrow batch as a human-oriented Unicode table.
///
/// # Errors
///
/// Returns an error when the batch contains a type not yet supported by the
/// human renderer.
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
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("type checked")
                .value(row);
            Ok(truncate(value, options.string_truncate))
        }
        DataType::Decimal128(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("type checked")
                .value(row);
            Ok(decimal(value, *scale, options.decimal_places))
        }
        data_type => Err(OutputError(format!(
            "unsupported demo display type {data_type}"
        ))),
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
    let digits = rounded.to_string();
    let padded = format!("{:0>width$}", digits, width = shown_scale + 1);
    let split = padded.len() - shown_scale;
    let (whole, fraction) = padded.split_at(split);
    let sign = if negative { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn display_transformations_do_not_mutate_source_values() {
        assert_eq!(truncate("abcdef", 4), "abc…");
        assert_eq!(decimal(123_456_789, 6, 3), "123.457");
        assert_eq!(decimal(123_456_500, 6, 3), "123.456");
        assert_eq!(decimal(123_455_500, 6, 3), "123.456");
    }
}

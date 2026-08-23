use std::borrow::Cow;

fn skip_ws(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t' | b'\r' | b'\n') {
        index += 1;
    }
    index
}

fn trim_ascii_ws(bytes: &[u8], mut start: usize, mut end: usize) -> (usize, usize) {
    while start < end && matches!(bytes[start], b' ' | b'\t' | b'\r' | b'\n') {
        start += 1;
    }
    while end > start && matches!(bytes[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    (start, end)
}

fn find_json_string_end(bytes: &[u8], start: usize) -> pyo3::PyResult<usize> {
    if start >= bytes.len() || bytes[start] != b'"' {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "expected JSON string",
        ));
    }

    let mut index = start + 1;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            let mut backslash_count = 0usize;
            let mut cursor = index;
            while cursor > start && bytes[cursor - 1] == b'\\' {
                backslash_count += 1;
                cursor -= 1;
            }
            if backslash_count & 1 == 0 {
                return Ok(index + 1);
            }
        }
        index += 1;
    }

    Err(pyo3::exceptions::PyValueError::new_err(
        "unterminated JSON string",
    ))
}

fn decode_json_string(raw: &str, kind: &str) -> pyo3::PyResult<String> {
    serde_json::from_str::<String>(raw).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid JSON {kind} string wrapper: {err}"
        ))
    })
}

fn normalize_array_input<'a>(raw: &'a str, kind: &str) -> pyo3::PyResult<Cow<'a, str>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "empty JSON {kind} array"
        )));
    }

    if trimmed.starts_with('[') {
        if trimmed.contains("\\\"") {
            return Ok(Cow::Owned(
                trimmed.replace("\\\\\"", "\"").replace("\\\"", "\""),
            ));
        }
        return Ok(Cow::Borrowed(trimmed));
    }

    if trimmed.starts_with('"') {
        let decoded = decode_json_string(trimmed, kind)?;
        let decoded_trimmed = decoded.trim();
        if decoded_trimmed.starts_with('[') {
            return Ok(Cow::Owned(decoded_trimmed.to_string()));
        }
    }

    let normalized = trimmed.replace("\\\\\"", "\"").replace("\\\"", "\"");
    if normalized.starts_with('[') {
        return Ok(Cow::Owned(normalized));
    }

    Err(pyo3::exceptions::PyValueError::new_err(format!(
        "invalid JSON {kind} array"
    )))
}

fn parse_i32_token(raw: &str) -> pyo3::PyResult<i32> {
    raw.parse::<i32>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid integer '{raw}': {err}"))
    })
}

fn parse_f64_token(raw: &str) -> pyo3::PyResult<f64> {
    raw.parse::<f64>().map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid float '{raw}': {err}"))
    })
}

fn parse_string_token(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<String> {
    let raw = std::str::from_utf8(&bytes[start..end]).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid utf-8 in JSON string token: {err}"
        ))
    })?;
    decode_json_string(raw, "string")
}

fn parse_json_array_tokens<T, F>(
    raw: &str,
    kind: &str,
    mut parse_token: F,
) -> pyo3::PyResult<Vec<T>>
where
    F: FnMut(&[u8], usize, usize) -> pyo3::PyResult<T>,
{
    let normalized = normalize_array_input(raw, kind)?;
    let bytes = normalized.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'[' || bytes[bytes.len() - 1] != b']' {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "expected JSON array for {kind} list"
        )));
    }

    let mut result = Vec::new();
    let mut index = skip_ws(bytes, 1);
    let end = bytes.len() - 1;
    while index < end {
        if bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
            continue;
        }

        let token_end = if bytes[index] == b'"' {
            find_json_string_end(bytes, index)?
        } else {
            bytes[index..end]
                .iter()
                .position(|byte| *byte == b',')
                .map(|offset| index + offset)
                .unwrap_or(end)
        };
        result.push(parse_token(bytes, index, token_end)?);
        index = skip_ws(bytes, token_end);
        if index < end && bytes[index] == b',' {
            index = skip_ws(bytes, index + 1);
        }
    }
    Ok(result)
}

fn parse_i32_token_from_bytes(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<i32> {
    let (start, end) = trim_ascii_ws(bytes, start, end);
    if start >= end {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "empty integer token",
        ));
    }
    let raw_token = if bytes[start] == b'"' {
        parse_string_token(bytes, start, end)?
    } else {
        std::str::from_utf8(&bytes[start..end])
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in int list: {err}"))
            })?
            .to_string()
    };
    parse_i32_token(&raw_token)
}

fn parse_f64_token_from_bytes(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<f64> {
    let (start, end) = trim_ascii_ws(bytes, start, end);
    if start >= end {
        return Err(pyo3::exceptions::PyValueError::new_err("empty float token"));
    }
    let raw_token = if bytes[start] == b'"' {
        parse_string_token(bytes, start, end)?
    } else {
        std::str::from_utf8(&bytes[start..end])
            .map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid utf-8 in float list: {err}"
                ))
            })?
            .to_string()
    };
    parse_f64_token(&raw_token)
}

#[allow(dead_code)]
fn parse_string_token_from_bytes(bytes: &[u8], start: usize, end: usize) -> pyo3::PyResult<String> {
    let (start, end) = trim_ascii_ws(bytes, start, end);
    if start >= end {
        return Ok(String::new());
    }
    if bytes[start] == b'"' {
        return parse_string_token(bytes, start, end);
    }
    std::str::from_utf8(&bytes[start..end])
        .map(str::to_string)
        .map_err(|err| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid utf-8 in string list: {err}"))
        })
}

pub fn parse_json_i32_array(raw: &str) -> pyo3::PyResult<Vec<i32>> {
    parse_json_array_tokens(raw, "int", parse_i32_token_from_bytes)
}

pub fn parse_json_f64_array(raw: &str) -> pyo3::PyResult<Vec<f64>> {
    parse_json_array_tokens(raw, "float", parse_f64_token_from_bytes)
}

#[allow(dead_code)]
pub fn parse_json_string_array(raw: &str) -> pyo3::PyResult<Vec<String>> {
    parse_json_array_tokens(raw, "string", parse_string_token_from_bytes)
}

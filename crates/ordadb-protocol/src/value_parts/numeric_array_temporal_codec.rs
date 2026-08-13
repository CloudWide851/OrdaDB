
fn encode_numeric_binary(value: Decimal) -> Result<Vec<u8>> {
    let scale = value.scale();
    let mut text = value.abs().to_string();
    if scale > 0 && !text.contains('.') {
        text.push('.');
        text.push_str(&"0".repeat(usize::try_from(scale).unwrap_or(0)));
    }
    let (integer, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let integer_padding = (4 - integer.len() % 4) % 4;
    let mut padded_integer = String::with_capacity(integer_padding + integer.len());
    padded_integer.push_str(&"0".repeat(integer_padding));
    padded_integer.push_str(integer);
    let fraction_padding = (4 - fraction.len() % 4) % 4;
    let mut padded_fraction = String::with_capacity(fraction.len() + fraction_padding);
    padded_fraction.push_str(fraction);
    padded_fraction.push_str(&"0".repeat(fraction_padding));
    let integer_groups = padded_integer.len() / 4;
    let mut digits = padded_integer
        .as_bytes()
        .chunks_exact(4)
        .chain(padded_fraction.as_bytes().chunks_exact(4))
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .map_err(|_| DbError::internal("numeric encoder generated invalid UTF-8"))?
                .parse::<u16>()
                .map_err(|_| DbError::internal("numeric encoder generated an invalid digit"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut weight = i16::try_from(integer_groups)
        .map_err(|_| DbError::new("22003", "numeric weight exceeds i16"))?
        .checked_sub(1)
        .ok_or_else(|| DbError::internal("numeric integer group count underflowed"))?;
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight = weight
            .checked_sub(1)
            .ok_or_else(|| DbError::new("22003", "numeric weight is out of range"))?;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }
    let ndigits = i16::try_from(digits.len())
        .map_err(|_| DbError::new("22003", "numeric digit count exceeds i16"))?;
    let dscale =
        u16::try_from(scale).map_err(|_| DbError::new("22003", "numeric scale exceeds u16"))?;
    let sign = if value.is_sign_negative() && !value.is_zero() {
        NUMERIC_NEGATIVE
    } else {
        NUMERIC_POSITIVE
    };
    let mut output = Vec::with_capacity(8 + digits.len() * 2);
    output.extend_from_slice(&ndigits.to_be_bytes());
    output.extend_from_slice(&weight.to_be_bytes());
    output.extend_from_slice(&sign.to_be_bytes());
    output.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        output.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(output)
}

fn decode_numeric_binary(bytes: &[u8]) -> Result<Decimal> {
    if bytes.len() < 8 || bytes.len() % 2 != 0 {
        return Err(protocol("binary numeric payload has an invalid length"));
    }
    let mut cursor = NetworkCursor::new(bytes);
    let ndigits = cursor.read_i16()?;
    let weight = cursor.read_i16()?;
    let sign = cursor.read_u16()?;
    let dscale = cursor.read_u16()?;
    if ndigits < 0 {
        return Err(protocol("binary numeric digit count is negative"));
    }
    if sign == NUMERIC_NAN {
        return Err(DbError::new(
            "0A000",
            "numeric NaN is not supported by the current decimal representation",
        ));
    }
    if !matches!(sign, NUMERIC_POSITIVE | NUMERIC_NEGATIVE) {
        return Err(protocol("binary numeric sign is invalid"));
    }
    if dscale > 28 {
        return Err(DbError::new(
            "22003",
            "numeric scale exceeds the current maximum of 28 digits",
        ));
    }
    let ndigits = usize::try_from(ndigits)
        .map_err(|_| protocol("binary numeric digit count is not addressable"))?;
    if cursor.remaining() != ndigits.saturating_mul(2) {
        return Err(protocol(
            "binary numeric digit count does not match its payload",
        ));
    }
    let mut digits = Vec::with_capacity(ndigits);
    for _ in 0..ndigits {
        let digit = cursor.read_u16()?;
        if digit >= 10_000 {
            return Err(protocol("binary numeric base-10000 digit is out of range"));
        }
        digits.push(digit);
    }
    cursor.finish()?;

    let integer_groups = if weight >= 0 {
        usize::try_from(i32::from(weight) + 1)
            .map_err(|_| DbError::new("22003", "numeric weight is out of range"))?
    } else {
        0
    };
    let fraction_groups = usize::from(dscale).div_ceil(4);
    let mut text = String::new();
    if sign == NUMERIC_NEGATIVE && digits.iter().any(|digit| *digit != 0) {
        text.push('-');
    }
    if integer_groups == 0 {
        text.push('0');
    } else {
        for position in 0..integer_groups {
            let exponent = i32::try_from(integer_groups - position - 1)
                .map_err(|_| DbError::new("22003", "numeric exponent is out of range"))?;
            let digit_index = i32::from(weight) - exponent;
            let digit = if digit_index >= 0 {
                usize::try_from(digit_index)
                    .ok()
                    .and_then(|index| digits.get(index))
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };
            if position == 0 {
                text.push_str(&digit.to_string());
            } else {
                text.push_str(&format!("{digit:04}"));
            }
        }
    }
    if dscale > 0 {
        text.push('.');
        let mut fraction = String::with_capacity(fraction_groups * 4);
        for group in 0..fraction_groups {
            let exponent = -(i32::try_from(group)
                .map_err(|_| DbError::new("22003", "numeric exponent is out of range"))?
                + 1);
            let digit_index = i32::from(weight) - exponent;
            let digit = if digit_index >= 0 {
                usize::try_from(digit_index)
                    .ok()
                    .and_then(|index| digits.get(index))
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };
            fraction.push_str(&format!("{digit:04}"));
        }
        fraction.truncate(usize::from(dscale));
        text.push_str(&fraction);
    }
    Decimal::from_str(&text).map_err(|_| DbError::new("22003", "binary numeric is out of range"))
}

fn array_oid(element: &ScalarType) -> Option<u32> {
    match element {
        ScalarType::Boolean => Some(OID_BOOL_ARRAY),
        ScalarType::Int16 => Some(OID_INT2_ARRAY),
        ScalarType::Int32 => Some(OID_INT4_ARRAY),
        ScalarType::Int64 => Some(OID_INT8_ARRAY),
        ScalarType::Oid => Some(OID_OID_ARRAY),
        ScalarType::Name => Some(OID_NAME_ARRAY),
        ScalarType::InternalChar => Some(OID_INTERNAL_CHAR_ARRAY),
        ScalarType::Float32 => Some(OID_FLOAT4_ARRAY),
        ScalarType::Float64 => Some(OID_FLOAT8_ARRAY),
        ScalarType::Decimal { .. } => Some(OID_NUMERIC_ARRAY),
        ScalarType::Char { .. } => Some(OID_BPCHAR_ARRAY),
        ScalarType::Varchar { .. } => Some(OID_VARCHAR_ARRAY),
        ScalarType::Text => Some(OID_TEXT_ARRAY),
        ScalarType::Enum { type_id, .. } => Some(enum_array_oid(*type_id)),
        ScalarType::Binary => Some(OID_BYTEA_ARRAY),
        ScalarType::Date => Some(OID_DATE_ARRAY),
        ScalarType::Time => Some(OID_TIME_ARRAY),
        ScalarType::Timestamp {
            with_timezone: false,
        } => Some(OID_TIMESTAMP_ARRAY),
        ScalarType::Timestamp {
            with_timezone: true,
        } => Some(OID_TIMESTAMPTZ_ARRAY),
        ScalarType::Interval => Some(OID_INTERVAL_ARRAY),
        ScalarType::Json => Some(OID_JSON_ARRAY),
        ScalarType::Jsonb => Some(OID_JSONB_ARRAY),
        ScalarType::Uuid => Some(OID_UUID_ARRAY),
        ScalarType::Array { .. } | ScalarType::Vector { .. } => None,
    }
}

fn array_element_type(oid: u32) -> Option<(ScalarType, u32)> {
    let value = match oid {
        OID_BOOL_ARRAY => (ScalarType::Boolean, OID_BOOL),
        OID_BYTEA_ARRAY => (ScalarType::Binary, OID_BYTEA),
        OID_INTERNAL_CHAR_ARRAY => (ScalarType::InternalChar, OID_INTERNAL_CHAR),
        OID_NAME_ARRAY => (ScalarType::Name, OID_NAME),
        OID_INT2_ARRAY => (ScalarType::Int16, OID_INT2),
        OID_INT4_ARRAY => (ScalarType::Int32, OID_INT4),
        OID_TEXT_ARRAY => (ScalarType::Text, OID_TEXT),
        OID_BPCHAR_ARRAY => (ScalarType::Char { length: None }, OID_BPCHAR),
        OID_VARCHAR_ARRAY => (ScalarType::Varchar { length: None }, OID_VARCHAR),
        OID_INT8_ARRAY => (ScalarType::Int64, OID_INT8),
        OID_OID_ARRAY => (ScalarType::Oid, OID_OID),
        OID_FLOAT4_ARRAY => (ScalarType::Float32, OID_FLOAT4),
        OID_FLOAT8_ARRAY => (ScalarType::Float64, OID_FLOAT8),
        OID_JSON_ARRAY => (ScalarType::Json, OID_JSON),
        OID_TIMESTAMP_ARRAY => (
            ScalarType::Timestamp {
                with_timezone: false,
            },
            OID_TIMESTAMP,
        ),
        OID_DATE_ARRAY => (ScalarType::Date, OID_DATE),
        OID_TIME_ARRAY => (ScalarType::Time, OID_TIME),
        OID_TIMESTAMPTZ_ARRAY => (
            ScalarType::Timestamp {
                with_timezone: true,
            },
            OID_TIMESTAMPTZ,
        ),
        OID_INTERVAL_ARRAY => (ScalarType::Interval, OID_INTERVAL),
        OID_NUMERIC_ARRAY => (
            ScalarType::Decimal {
                precision: None,
                scale: None,
            },
            OID_NUMERIC,
        ),
        OID_UUID_ARRAY => (ScalarType::Uuid, OID_UUID),
        OID_JSONB_ARRAY => (ScalarType::Jsonb, OID_JSONB),
        _ => return None,
    };
    Some(value)
}

fn decode_array_text(text: &str, element_type: ScalarType, element_oid: u32) -> Result<Value> {
    let mut parser = ArrayTextParser::new(text);
    let declared = parser.parse_dimensions()?;
    let (shape, elements) = parser.parse_level(0)?;
    parser.finish()?;
    let dimensions = if let Some(declared) = declared {
        if declared
            .iter()
            .map(|dimension| dimension.length)
            .collect::<Vec<_>>()
            != shape
                .iter()
                .map(|length| u32::try_from(*length).unwrap_or(u32::MAX))
                .collect::<Vec<_>>()
        {
            return Err(DbError::new(
                "22P02",
                "array dimensions do not match the array literal contents",
            ));
        }
        declared
    } else if shape == [0] {
        Vec::new()
    } else {
        shape
            .into_iter()
            .map(|length| {
                u32::try_from(length)
                    .map(|length| ArrayDimension::new(length, 1))
                    .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))
            })
            .collect::<Result<Vec<_>>>()?
    };
    let values = elements
        .into_iter()
        .map(|element| match element {
            None => Ok(Value::Null),
            Some(element) => match &element_type {
                ScalarType::Enum { labels, .. } => decode_enum(element.as_bytes(), labels),
                _ => decode_text(element_oid, element.as_bytes()),
            },
        })
        .collect::<Result<Vec<_>>>()?;
    PgArray::new(element_type, dimensions, values).map(Value::Array)
}

fn encode_array_text(array: &PgArray) -> Result<String> {
    let mut output = String::new();
    if array
        .dimensions()
        .iter()
        .any(|dimension| dimension.lower_bound != 1)
    {
        for dimension in array.dimensions() {
            let upper = i64::from(dimension.lower_bound)
                .checked_add(i64::from(dimension.length))
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| DbError::new("2202E", "array upper bound overflows"))?;
            output.push_str(&format!("[{}:{upper}]", dimension.lower_bound));
        }
        output.push('=');
    }
    if array.dimensions().is_empty() {
        output.push_str("{}");
        return Ok(output);
    }
    let mut offset = 0;
    encode_array_level(&mut output, array.dimensions(), array.values(), &mut offset)?;
    if offset != array.values().len() {
        return Err(DbError::internal(
            "array encoder did not consume every value",
        ));
    }
    Ok(output)
}

fn encode_array_level(
    output: &mut String,
    dimensions: &[ArrayDimension],
    values: &[Value],
    offset: &mut usize,
) -> Result<()> {
    let Some((dimension, remaining)) = dimensions.split_first() else {
        return Err(DbError::internal(
            "array encoder reached an empty dimension",
        ));
    };
    output.push('{');
    for index in 0..dimension.length {
        if index != 0 {
            output.push(',');
        }
        if remaining.is_empty() {
            let value = values
                .get(*offset)
                .ok_or_else(|| DbError::internal("array encoder ran out of values"))?;
            *offset += 1;
            output.push_str(&encode_array_element_text(value)?);
        } else {
            encode_array_level(output, remaining, values, offset)?;
        }
    }
    output.push('}');
    Ok(())
}

fn encode_array_element_text(value: &Value) -> Result<String> {
    if value.is_null() {
        return Ok("NULL".to_owned());
    }
    let bytes = encode_text(value)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| DbError::new("22021", "array element text is not valid UTF-8"))?;
    let needs_quotes = text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .chars()
            .any(|character| character.is_whitespace() || ",{}\"\\".contains(character));
    if !needs_quotes {
        return Ok(text.to_owned());
    }
    Ok(format!(
        "\"{}\"",
        text.replace('\\', "\\\\").replace('\"', "\\\"")
    ))
}

fn encode_array_binary(array: &PgArray, data_type: &ScalarType) -> Result<Vec<u8>> {
    let ScalarType::Array { element } = data_type else {
        return Err(DbError::new(
            "42804",
            "array value requires an array result type",
        ));
    };
    if element.as_ref() != array.element_type() {
        return Err(DbError::new(
            "42804",
            "array value element type does not match its result type",
        ));
    }
    let element_oid = type_oid(element);
    if element_oid == 0 {
        return Err(DbError::new(
            "0A000",
            "binary arrays are unsupported for this element type",
        ));
    }
    if array_oid(element).is_none() {
        return Err(DbError::new(
            "0A000",
            "binary arrays are unsupported for this element type",
        ));
    }
    let dimensions = i32::try_from(array.dimensions().len())
        .map_err(|_| DbError::new("54000", "array dimension count exceeds i32::MAX"))?;
    let mut output = Vec::new();
    output.extend_from_slice(&dimensions.to_be_bytes());
    output.extend_from_slice(&i32::from(array.values().iter().any(Value::is_null)).to_be_bytes());
    output.extend_from_slice(&element_oid.to_be_bytes());
    for dimension in array.dimensions() {
        output.extend_from_slice(
            &i32::try_from(dimension.length)
                .map_err(|_| DbError::new("54000", "array dimension exceeds i32::MAX"))?
                .to_be_bytes(),
        );
        output.extend_from_slice(&dimension.lower_bound.to_be_bytes());
    }
    for value in array.values() {
        if value.is_null() {
            output.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let encoded = encode_array_element_binary(value, element)?;
        output.extend_from_slice(
            &i32::try_from(encoded.len())
                .map_err(|_| DbError::new("54000", "array element exceeds i32::MAX bytes"))?
                .to_be_bytes(),
        );
        output.extend_from_slice(&encoded);
    }
    Ok(output)
}

fn encode_array_element_binary(value: &Value, element: &ScalarType) -> Result<Vec<u8>> {
    match (value, element) {
        (Value::Int16(value), ScalarType::Int32) => Ok(i32::from(*value).to_be_bytes().to_vec()),
        (Value::Int16(value), ScalarType::Int64) => Ok(i64::from(*value).to_be_bytes().to_vec()),
        (Value::Int32(value), ScalarType::Int64) => Ok(i64::from(*value).to_be_bytes().to_vec()),
        (Value::Int16(value), ScalarType::Float32) => {
            Ok(f32::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int16(value), ScalarType::Float64) => {
            Ok(f64::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int32(value), ScalarType::Float64) => {
            Ok(f64::from(*value).to_bits().to_be_bytes().to_vec())
        }
        (Value::Int64(value), ScalarType::Float64) => {
            Ok((*value as f64).to_bits().to_be_bytes().to_vec())
        }
        _ => encode_binary(value, element),
    }
}

fn decode_array_binary(bytes: &[u8], element_type: ScalarType, element_oid: u32) -> Result<Value> {
    let mut cursor = NetworkCursor::new(bytes);
    let dimension_count = cursor.read_i32()?;
    if dimension_count < 0
        || usize::try_from(dimension_count)
            .ok()
            .is_none_or(|count| count > MAX_ARRAY_DIMENSIONS)
    {
        return Err(protocol("binary array dimension count is out of range"));
    }
    let has_null = cursor.read_i32()?;
    if !matches!(has_null, 0 | 1) {
        return Err(protocol("binary array NULL flag must be zero or one"));
    }
    if cursor.read_u32()? != element_oid {
        return Err(protocol(
            "binary array element OID does not match its array OID",
        ));
    }
    let mut dimensions = Vec::with_capacity(usize::try_from(dimension_count).unwrap_or(0));
    let mut element_count = if dimension_count == 0 {
        0_usize
    } else {
        1_usize
    };
    for _ in 0..dimension_count {
        let length = cursor.read_i32()?;
        if length < 0 {
            return Err(protocol(
                "binary array dimension length must be non-negative",
            ));
        }
        let length = usize::try_from(length)
            .map_err(|_| protocol("binary array dimension length is not addressable"))?;
        element_count = element_count
            .checked_mul(length)
            .ok_or_else(|| DbError::new("54000", "binary array element count overflows"))?;
        if element_count > MAX_ARRAY_ELEMENTS {
            return Err(DbError::new(
                "54000",
                "binary array exceeds the maximum element count",
            ));
        }
        dimensions.push(ArrayDimension::new(
            u32::try_from(length)
                .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))?,
            cursor.read_i32()?,
        ));
    }
    let mut values = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let length = cursor.read_i32()?;
        if length == -1 {
            values.push(Value::Null);
            continue;
        }
        if length < 0 {
            return Err(protocol("binary array element length is invalid"));
        }
        let payload = cursor.take(
            usize::try_from(length)
                .map_err(|_| protocol("binary array element length is not addressable"))?,
        )?;
        values.push(match &element_type {
            ScalarType::Enum { labels, .. } => decode_enum(payload, labels)?,
            _ => decode_binary(element_oid, payload)?,
        });
    }
    cursor.finish()?;
    if has_null == 0 && values.iter().any(Value::is_null) {
        return Err(protocol(
            "binary array contains NULL despite its header flag",
        ));
    }
    PgArray::new(element_type, dimensions, values).map(Value::Array)
}

struct NetworkCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> NetworkCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("checked length"),
        ))
    }

    fn read_i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("checked length"),
        ))
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("checked length"),
        ))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("checked length"),
        ))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| protocol("binary array offset overflows"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| protocol("binary array payload is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(protocol("binary array payload has trailing bytes"));
        }
        Ok(())
    }
}

struct ArrayTextParser<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ArrayTextParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse_dimensions(&mut self) -> Result<Option<Vec<ArrayDimension>>> {
        self.skip_whitespace();
        if self.peek() != Some(b'[') {
            return Ok(None);
        }
        let mut dimensions = Vec::new();
        while self.peek() == Some(b'[') {
            self.offset += 1;
            let lower = self.parse_bound_integer()?;
            self.expect(b':')?;
            let upper = self.parse_bound_integer()?;
            self.expect(b']')?;
            let length = i64::from(upper)
                .checked_sub(i64::from(lower))
                .and_then(|value| value.checked_add(1))
                .filter(|value| *value >= 0)
                .ok_or_else(|| DbError::new("22P02", "array bounds are invalid"))?;
            dimensions.push(ArrayDimension::new(
                u32::try_from(length)
                    .map_err(|_| DbError::new("54000", "array dimension exceeds u32::MAX"))?,
                lower,
            ));
            if dimensions.len() > MAX_ARRAY_DIMENSIONS {
                return Err(DbError::new(
                    "54000",
                    "array exceeds the maximum dimension count",
                ));
            }
        }
        self.expect(b'=')?;
        Ok(Some(dimensions))
    }

    fn parse_level(&mut self, depth: usize) -> Result<(Vec<usize>, Vec<Option<String>>)> {
        if depth >= MAX_ARRAY_DIMENSIONS {
            return Err(DbError::new(
                "54000",
                "array exceeds the maximum dimension count",
            ));
        }
        self.skip_whitespace();
        self.expect(b'{')?;
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok((vec![0], Vec::new()));
        }
        let nested = self.peek() == Some(b'{');
        let mut count = 0_usize;
        let mut child_shape: Option<Vec<usize>> = None;
        let mut values = Vec::new();
        loop {
            self.skip_whitespace();
            if nested {
                if self.peek() != Some(b'{') {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional array has mixed scalar and nested elements",
                    ));
                }
                let (shape, mut child_values) = self.parse_level(depth + 1)?;
                if child_shape
                    .as_ref()
                    .is_some_and(|expected| expected != &shape)
                {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional arrays must have matching dimensions",
                    ));
                }
                child_shape.get_or_insert(shape);
                values.append(&mut child_values);
            } else {
                if self.peek() == Some(b'{') {
                    return Err(DbError::new(
                        "22P02",
                        "multidimensional array has mixed scalar and nested elements",
                    ));
                }
                values.push(self.parse_element()?);
            }
            count = count
                .checked_add(1)
                .ok_or_else(|| DbError::new("54000", "array element count overflows"))?;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    break;
                }
                _ => return Err(DbError::new("22P02", "array literal is malformed")),
            }
        }
        let mut shape = vec![count];
        if let Some(child_shape) = child_shape {
            shape.extend(child_shape);
        }
        Ok((shape, values))
    }

    fn parse_element(&mut self) -> Result<Option<String>> {
        if self.peek() == Some(b'\"') {
            return self.parse_quoted().map(Some);
        }
        let mut bytes = Vec::new();
        while let Some(byte) = self.peek() {
            if matches!(byte, b',' | b'}') {
                break;
            }
            self.offset += 1;
            if byte == b'\\' {
                let escaped = self
                    .peek()
                    .ok_or_else(|| DbError::new("22P02", "array element ends with an escape"))?;
                self.offset += 1;
                bytes.push(escaped);
            } else {
                bytes.push(byte);
            }
        }
        let value = std::str::from_utf8(&bytes)
            .map_err(|_| DbError::new("22021", "array element is not valid UTF-8"))?
            .trim()
            .to_owned();
        if value.is_empty() {
            return Err(DbError::new("22P02", "unquoted array element is empty"));
        }
        Ok((!value.eq_ignore_ascii_case("NULL")).then_some(value))
    }

    fn parse_quoted(&mut self) -> Result<String> {
        self.expect(b'\"')?;
        let mut bytes = Vec::new();
        loop {
            let byte = self
                .peek()
                .ok_or_else(|| DbError::new("22P02", "quoted array element is unterminated"))?;
            self.offset += 1;
            match byte {
                b'\"' => break,
                b'\\' => {
                    let escaped = self.peek().ok_or_else(|| {
                        DbError::new("22P02", "quoted array element ends with an escape")
                    })?;
                    self.offset += 1;
                    bytes.push(escaped);
                }
                _ => bytes.push(byte),
            }
        }
        String::from_utf8(bytes)
            .map_err(|_| DbError::new("22021", "quoted array element is not valid UTF-8"))
    }

    fn parse_bound_integer(&mut self) -> Result<i32> {
        let start = self.offset;
        if matches!(self.peek(), Some(b'-' | b'+')) {
            self.offset += 1;
        }
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.offset += 1;
        }
        let value = std::str::from_utf8(&self.bytes[start..self.offset])
            .map_err(|_| DbError::new("22P02", "array bound is not valid UTF-8"))?;
        value
            .parse()
            .map_err(|_| DbError::new("22P02", "array bound is invalid"))
    }

    fn expect(&mut self, expected: u8) -> Result<()> {
        self.skip_whitespace();
        if self.peek() != Some(expected) {
            return Err(DbError::new(
                "22P02",
                format!("array literal expected `{}`", char::from(expected)),
            ));
        }
        self.offset += 1;
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn finish(mut self) -> Result<()> {
        self.skip_whitespace();
        if self.offset != self.bytes.len() {
            return Err(DbError::new("22P02", "array literal has trailing input"));
        }
        Ok(())
    }
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc3339(text)
                .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%:z"))
                .ok()
                .map(|value| value.with_timezone(&Utc).naive_utc())
        })
}

fn decode_bytea_text(text: &str) -> Result<Vec<u8>> {
    let Some(hex) = text.strip_prefix("\\x") else {
        return Ok(text.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return Err(DbError::new(
            "22P02",
            "hex bytea has an odd number of digits",
        ));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(DbError::new("22P02", "bytea contains a non-hex digit")),
    }
}

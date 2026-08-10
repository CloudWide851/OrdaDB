use std::collections::BTreeSet;

use ordadb_types::{DbError, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::{AiToolCall, AiToolDefinition, ValidatedAiToolCall};

const MAX_SCHEMA_DEPTH: usize = 32;
const MAX_SCHEMA_NODES: usize = 4_096;
const MAX_TOOL_NAME_BYTES: usize = 64;
const MAX_TOOL_DESCRIPTION_BYTES: usize = 4_096;
const MAX_CALL_ID_BYTES: usize = 256;
const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;

pub fn validate_tool_definition(definition: &AiToolDefinition) -> Result<()> {
    validate_identifier(&definition.name, MAX_TOOL_NAME_BYTES, "AI tool name")?;
    validate_text(
        &definition.description,
        MAX_TOOL_DESCRIPTION_BYTES,
        "AI tool description",
    )?;
    let mut nodes = 0;
    validate_schema(&definition.parameters, 0, &mut nodes)?;
    Ok(())
}

pub fn validate_tool_arguments(
    definition: &AiToolDefinition,
    call: AiToolCall,
) -> Result<ValidatedAiToolCall> {
    validate_tool_definition(definition)?;
    validate_identifier(&call.call_id, MAX_CALL_ID_BYTES, "AI tool call ID")?;
    if call.name != definition.name {
        return Err(invalid(
            "AI tool call name does not match its registered definition",
        ));
    }
    let canonical_arguments = canonical_json(&call.arguments)?;
    if canonical_arguments.len() > MAX_ARGUMENT_BYTES {
        return Err(limit("AI tool arguments exceed the 1 MiB limit"));
    }
    let mut nodes = 0;
    validate_value_against_schema(
        &call.arguments,
        &definition.parameters,
        0,
        &mut nodes,
        "arguments",
    )?;
    Ok(ValidatedAiToolCall::new(
        call.call_id,
        definition.clone(),
        call.arguments,
        canonical_arguments,
    ))
}

pub fn canonical_json(value: &JsonValue) -> Result<String> {
    let canonical = canonical_value(value, 0)?;
    serde_json::to_string(&canonical).map_err(|error| {
        DbError::new("XX000", "failed to serialize canonical AI tool arguments")
            .with_detail(error.to_string())
    })
}

pub fn canonical_json_hash(value: &JsonValue) -> Result<[u8; 32]> {
    let canonical = canonical_json(value)?;
    Ok(Sha256::digest(canonical.as_bytes()).into())
}

fn canonical_value(value: &JsonValue, depth: usize) -> Result<JsonValue> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(limit("AI JSON nesting exceeds the depth limit"));
    }
    Ok(match value {
        JsonValue::Array(values) => JsonValue::Array(
            values
                .iter()
                .map(|value| canonical_value(value, depth + 1))
                .collect::<Result<Vec<_>>>()?,
        ),
        JsonValue::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut canonical = JsonMap::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_value(&values[key], depth + 1)?);
            }
            JsonValue::Object(canonical)
        }
        scalar => scalar.clone(),
    })
}

fn validate_schema(schema: &JsonValue, depth: usize, nodes: &mut usize) -> Result<()> {
    count_node(depth, nodes, "AI tool schema")?;
    let object = schema
        .as_object()
        .ok_or_else(|| invalid("AI tool schema must be an object"))?;
    let schema_type = object
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| invalid("AI tool schema must declare a string type"))?;
    match schema_type {
        "object" => {
            let properties = object
                .get("properties")
                .and_then(JsonValue::as_object)
                .ok_or_else(|| invalid("strict AI object schema requires properties"))?;
            if object.get("additionalProperties") != Some(&JsonValue::Bool(false)) {
                return Err(invalid(
                    "strict AI object schema requires additionalProperties=false",
                ));
            }
            let required = object
                .get("required")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| invalid("strict AI object schema requires required"))?;
            let required = required
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| invalid("AI schema required entries must be strings"))
                })
                .collect::<Result<BTreeSet<_>>>()?;
            let property_names = properties
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            if required != property_names {
                return Err(invalid(
                    "strict AI object schema must require every declared property exactly once",
                ));
            }
            for property in properties.values() {
                validate_schema(property, depth + 1, nodes)?;
            }
        }
        "array" => {
            let items = object
                .get("items")
                .ok_or_else(|| invalid("AI array schema requires items"))?;
            validate_schema(items, depth + 1, nodes)?;
        }
        "string" | "number" | "integer" | "boolean" | "null" => {}
        _ => return Err(invalid("AI tool schema uses an unsupported type")),
    }
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .ok_or_else(|| invalid("AI schema enum must be an array"))?;
        if values.is_empty() || values.len() > 256 {
            return Err(limit(
                "AI schema enum must contain between 1 and 256 values",
            ));
        }
    }
    Ok(())
}

fn validate_value_against_schema(
    value: &JsonValue,
    schema: &JsonValue,
    depth: usize,
    nodes: &mut usize,
    path: &str,
) -> Result<()> {
    count_node(depth, nodes, "AI tool arguments")?;
    let schema = schema
        .as_object()
        .ok_or_else(|| invalid("AI tool schema must be an object"))?;
    if let Some(allowed) = schema.get("enum").and_then(JsonValue::as_array)
        && !allowed.contains(value)
    {
        return Err(invalid(format!("{path} is not an allowed enum value")));
    }
    match schema.get("type").and_then(JsonValue::as_str) {
        Some("object") => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid(format!("{path} must be an object")))?;
            let properties = schema["properties"]
                .as_object()
                .ok_or_else(|| invalid("AI object schema requires properties"))?;
            if object.len() != properties.len()
                || object.keys().any(|key| !properties.contains_key(key))
            {
                return Err(invalid(format!(
                    "{path} contains missing or unknown fields"
                )));
            }
            for (name, property_schema) in properties {
                validate_value_against_schema(
                    &object[name],
                    property_schema,
                    depth + 1,
                    nodes,
                    &format!("{path}.{name}"),
                )?;
            }
        }
        Some("array") => {
            let array = value
                .as_array()
                .ok_or_else(|| invalid(format!("{path} must be an array")))?;
            if array.len() > 10_000 {
                return Err(limit(format!("{path} exceeds the array item limit")));
            }
            for (index, item) in array.iter().enumerate() {
                validate_value_against_schema(
                    item,
                    &schema["items"],
                    depth + 1,
                    nodes,
                    &format!("{path}[{index}]"),
                )?;
            }
        }
        Some("string") if !value.is_string() => {
            return Err(invalid(format!("{path} must be a string")));
        }
        Some("number") if !value.is_number() => {
            return Err(invalid(format!("{path} must be a number")));
        }
        Some("integer") if value.as_i64().is_none() && value.as_u64().is_none() => {
            return Err(invalid(format!("{path} must be an integer")));
        }
        Some("boolean") if !value.is_boolean() => {
            return Err(invalid(format!("{path} must be a boolean")));
        }
        Some("null") if !value.is_null() => {
            return Err(invalid(format!("{path} must be null")));
        }
        Some("string" | "number" | "integer" | "boolean" | "null") => {}
        _ => return Err(invalid("AI tool schema uses an unsupported type")),
    }
    Ok(())
}

fn count_node(depth: usize, nodes: &mut usize, context: &str) -> Result<()> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(limit(format!("{context} exceeds the depth limit")));
    }
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_SCHEMA_NODES {
        return Err(limit(format!("{context} exceeds the node limit")));
    }
    Ok(())
}

fn validate_identifier(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum
        || value.as_bytes().contains(&0)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, context: &str) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(invalid(format!("{context} is invalid")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DbError {
    DbError::new("22023", message)
}

fn limit(message: impl Into<String>) -> DbError {
    DbError::new("54000", message)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::AiToolRisk;

    fn definition() -> AiToolDefinition {
        AiToolDefinition {
            name: "read_catalog".to_owned(),
            version: 1,
            description: "Read bounded catalog metadata".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "schema": {"type": "string"},
                    "limit": {"type": "integer"}
                },
                "required": ["schema", "limit"],
                "additionalProperties": false
            }),
            risk: AiToolRisk::ReadOnly,
        }
    }

    #[test]
    fn strict_schema_and_arguments_are_validated_and_canonicalized() {
        let definition = definition();
        validate_tool_definition(&definition).expect("definition");
        let call = AiToolCall {
            call_id: "call-1".to_owned(),
            name: definition.name.clone(),
            arguments: json!({"limit": 25, "schema": "public"}),
        };
        let validated = validate_tool_arguments(&definition, call).expect("arguments");
        assert_eq!(
            validated.canonical_arguments(),
            r#"{"limit":25,"schema":"public"}"#
        );
        assert!(!format!("{validated:?}").contains("public"));
    }

    #[test]
    fn strict_schema_rejects_optional_or_additional_properties() {
        let mut invalid_definition = definition();
        invalid_definition.parameters["required"] = json!(["schema"]);
        assert_eq!(
            validate_tool_definition(&invalid_definition)
                .expect_err("optional property")
                .sql_state,
            "22023"
        );

        let definition = definition();
        let call = AiToolCall {
            call_id: "call-2".to_owned(),
            name: definition.name.clone(),
            arguments: json!({"schema": "public", "limit": 25, "secret": true}),
        };
        assert_eq!(
            validate_tool_arguments(&definition, call)
                .expect_err("unknown argument")
                .sql_state,
            "22023"
        );
    }

    #[test]
    fn canonical_json_sorts_every_object_level() {
        let canonical = canonical_json(&json!({
            "z": [{"b": 2, "a": 1}],
            "a": {"d": 4, "c": 3}
        }))
        .expect("canonical");
        assert_eq!(canonical, r#"{"a":{"c":3,"d":4},"z":[{"a":1,"b":2}]}"#);
    }
}

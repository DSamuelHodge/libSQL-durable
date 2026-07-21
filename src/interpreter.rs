//! Minimal `pvm.def.v1` interpreter.
//!
//! Control flow comes from definition JSON in the world. Activities remain
//! host-supplied. The interpreter only schedules journaled Duroxide ops.

use std::collections::HashMap;
use std::time::Duration;

use duroxide::OrchestrationContext;
use duroxide::runtime::registry::OrchestrationRegistry;
use serde_json::Value as JsonValue;

use crate::definitions::{PVM_DEF_V1, validate_definition_body};

/// Orchestration name registered for interpreted processes.
pub const INTERPRETED_ORCH_NAME: &str = "pvm.interpret";

/// Build a registry that contains only the generic `pvm.interpret` process.
///
/// Input format (JSON string):
/// ```json
/// { "body": { ... pvm.def.v1 ... }, "input": "..." }
/// ```
/// or plain text input with body loaded by the host before start (see
/// `wrap_interpret_input`).
pub fn interpreted_orchestrations() -> OrchestrationRegistry {
    let interpret = |ctx: OrchestrationContext, payload: String| async move {
        run_pvm_def_v1(&ctx, &payload).await
    };
    OrchestrationRegistry::builder()
        .register(INTERPRETED_ORCH_NAME, interpret)
        .build()
}

/// Wrap a definition body + process input for `pvm.interpret`.
pub fn wrap_interpret_input(body_json: &str, process_input: &str) -> Result<String, String> {
    validate_definition_body(body_json)?;
    let body: JsonValue = serde_json::from_str(body_json).map_err(|e| e.to_string())?;
    if body.get("schema").and_then(|s| s.as_str()) != Some(PVM_DEF_V1) {
        return Err(format!("interpreter requires schema {PVM_DEF_V1}"));
    }
    let wrapped = serde_json::json!({
        "body": body,
        "input": process_input,
    });
    Ok(wrapped.to_string())
}

async fn run_pvm_def_v1(ctx: &OrchestrationContext, payload: &str) -> Result<String, String> {
    let root: JsonValue =
        serde_json::from_str(payload).map_err(|e| format!("interpret payload JSON: {e}"))?;
    let body = root
        .get("body")
        .cloned()
        .ok_or_else(|| "interpret payload missing `body`".to_string())?;
    let process_input = root
        .get("input")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let body_str = serde_json::to_string(&body).map_err(|e| e.to_string())?;
    validate_definition_body(&body_str)?;

    let entry = body
        .get("entry")
        .and_then(|e| e.as_str())
        .ok_or("missing entry")?
        .to_string();
    let steps = body
        .get("steps")
        .and_then(|s| s.as_array())
        .ok_or("missing steps")?;

    let mut by_id: HashMap<String, JsonValue> = HashMap::new();
    for step in steps {
        let id = step
            .get("id")
            .and_then(|x| x.as_str())
            .ok_or("step missing id")?
            .to_string();
        by_id.insert(id, step.clone());
    }

    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("input".into(), process_input);

    let mut current = entry;
    // Bound steps to avoid infinite goto loops in a single execution.
    for _ in 0..10_000 {
        let step = by_id
            .get(&current)
            .ok_or_else(|| format!("unknown step id `{current}`"))?;
        let op = step
            .get("op")
            .and_then(|o| o.as_str())
            .ok_or("step missing op")?;

        match op {
            "activity" => {
                let name = step
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or("activity missing name")?;
                let input = resolve_value(step.get("input"), &vars);
                let out = ctx.schedule_activity(name, input).await?;
                if let Some(out_name) = step.get("out").and_then(|o| o.as_str()) {
                    vars.insert(out_name.to_string(), out.clone());
                }
                current = next_step(step, &current, &by_id)?;
            }
            "timer" => {
                let ms = step.get("ms").and_then(|m| m.as_u64()).unwrap_or(0);
                ctx.schedule_timer(Duration::from_millis(ms)).await;
                current = next_step(step, &current, &by_id)?;
            }
            "wait" => {
                let event = step
                    .get("event")
                    .and_then(|e| e.as_str())
                    .ok_or("wait missing event")?;
                let data = ctx.schedule_wait(event).await;
                if let Some(out_name) = step.get("out").and_then(|o| o.as_str()) {
                    vars.insert(out_name.to_string(), data);
                }
                current = next_step(step, &current, &by_id)?;
            }
            "set_kv" => {
                let key = step
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or("set_kv missing key")?;
                let value = resolve_value(step.get("value"), &vars);
                ctx.set_kv_value(key, &value);
                current = next_step(step, &current, &by_id)?;
            }
            "set_status" => {
                let value = resolve_value(step.get("value"), &vars);
                ctx.set_custom_status(&value);
                current = next_step(step, &current, &by_id)?;
            }
            "goto" => {
                let target = step
                    .get("target")
                    .and_then(|t| t.as_str())
                    .ok_or("goto missing target")?;
                current = target.to_string();
            }
            "return" => {
                return Ok(resolve_value(step.get("value"), &vars));
            }
            other => return Err(format!("unsupported op `{other}`")),
        }
    }
    Err("interpreter step limit exceeded (possible goto loop)".into())
}

fn resolve_value(v: Option<&JsonValue>, vars: &HashMap<String, String>) -> String {
    match v {
        None => String::new(),
        Some(JsonValue::String(s)) if s.starts_with('$') => {
            let key = &s[1..];
            vars.get(key).cloned().unwrap_or_default()
        }
        Some(JsonValue::String(s)) => s.clone(),
        Some(JsonValue::Number(n)) => n.to_string(),
        Some(JsonValue::Bool(b)) => b.to_string(),
        Some(other) => other.to_string(),
    }
}

/// Linear next: explicit `next` field, else following step in insertion order is not known —
/// require `next` or treat as end error. For v1 we use optional `next`; if absent, fail unless return.
fn next_step(
    step: &JsonValue,
    current: &str,
    by_id: &HashMap<String, JsonValue>,
) -> Result<String, String> {
    if let Some(n) = step.get("next").and_then(|x| x.as_str()) {
        return Ok(n.to_string());
    }
    // Convention: if no next, look for a step that is not current — not reliable.
    // Require explicit next for multi-step graphs; single-step should use return.
    let _ = by_id;
    Err(format!(
        "step `{current}` missing `next` (use `return` to finish or set `next`)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_requires_v1_schema() {
        let body = r#"{"schema":"pvm.def.v1","entry":"main","steps":[{"id":"main","op":"return","value":"$input"}]}"#;
        let w = wrap_interpret_input(body, "hi").unwrap();
        assert!(w.contains("pvm.def.v1"));
        assert!(w.contains("hi"));
    }
}

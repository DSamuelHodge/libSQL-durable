//! `pvm.def.v1` interpreter — portable process graphs as data.
//!
//! Control flow comes from definition JSON in the world. Activities remain
//! host-supplied. The interpreter only schedules journaled Duroxide ops
//! (including `if` branches and `select` races).

use std::collections::HashMap;
use std::time::Duration;

use duroxide::runtime::registry::OrchestrationRegistry;
use duroxide::{Either2, OrchestrationContext};
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
    // Bound steps to avoid infinite goto / if loops in a single execution.
    for _ in 0..10_000 {
        let step = by_id
            .get(&current)
            .ok_or_else(|| format!("unknown step id `{current}`"))?
            .clone();
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
                    vars.insert(out_name.to_string(), out);
                }
                current = next_step(&step, &current)?;
            }
            "timer" => {
                let ms = step.get("ms").and_then(|m| m.as_u64()).unwrap_or(0);
                ctx.schedule_timer(Duration::from_millis(ms)).await;
                current = next_step(&step, &current)?;
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
                current = next_step(&step, &current)?;
            }
            "set_kv" => {
                let key = step
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or("set_kv missing key")?;
                let value = resolve_value(step.get("value"), &vars);
                ctx.set_kv_value(key, &value);
                current = next_step(&step, &current)?;
            }
            "set_status" => {
                let value = resolve_value(step.get("value"), &vars);
                ctx.set_custom_status(&value);
                current = next_step(&step, &current)?;
            }
            "goto" => {
                let target = step
                    .get("target")
                    .and_then(|t| t.as_str())
                    .ok_or("goto missing target")?;
                current = target.to_string();
            }
            "if" => {
                let then_id = step
                    .get("then")
                    .and_then(|t| t.as_str())
                    .ok_or("if missing then")?;
                let else_id = step
                    .get("else")
                    .and_then(|t| t.as_str())
                    .ok_or("if missing else")?;
                current = if eval_cond(step.get("cond"), &vars)? {
                    then_id.to_string()
                } else {
                    else_id.to_string()
                };
            }
            "select" => {
                current = run_select(ctx, &step, &mut vars).await?;
            }
            "return" => {
                return Ok(resolve_value(step.get("value"), &vars));
            }
            other => return Err(format!("unsupported op `{other}`")),
        }
    }
    Err("interpreter step limit exceeded (possible goto/if loop)".into())
}

#[derive(Clone)]
enum PreparedArm {
    Timer {
        ms: u64,
        value: String,
        next: String,
        out: Option<String>,
    },
    Wait {
        event: String,
        next: String,
        out: Option<String>,
    },
    Activity {
        name: String,
        input: String,
        next: String,
        out: Option<String>,
    },
}

impl PreparedArm {
    fn out_name(&self) -> Option<&str> {
        match self {
            Self::Timer { out, .. } | Self::Wait { out, .. } | Self::Activity { out, .. } => {
                out.as_deref()
            }
        }
    }

    fn prepare(arm: &JsonValue, vars: &HashMap<String, String>) -> Result<Self, String> {
        let kind = arm
            .get("kind")
            .and_then(|k| k.as_str())
            .ok_or("arm missing kind")?;
        let next = arm
            .get("next")
            .and_then(|n| n.as_str())
            .ok_or("arm missing next")?
            .to_string();
        let out = arm
            .get("out")
            .and_then(|o| o.as_str())
            .map(|s| s.to_string());
        match kind {
            "timer" => {
                let ms = arm.get("ms").and_then(|m| m.as_u64()).unwrap_or(0);
                let mut value = resolve_value(arm.get("value"), vars);
                if value.is_empty() {
                    value = "timer".into();
                }
                Ok(Self::Timer {
                    ms,
                    value,
                    next,
                    out,
                })
            }
            "wait" => {
                let event = arm
                    .get("event")
                    .and_then(|e| e.as_str())
                    .ok_or("wait arm missing event")?
                    .to_string();
                Ok(Self::Wait { event, next, out })
            }
            "activity" => {
                let name = arm
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or("activity arm missing name")?
                    .to_string();
                let input = resolve_value(arm.get("input"), vars);
                Ok(Self::Activity {
                    name,
                    input,
                    next,
                    out,
                })
            }
            other => Err(format!("unknown select arm kind `{other}`")),
        }
    }
}

async fn run_select(
    ctx: &OrchestrationContext,
    step: &JsonValue,
    vars: &mut HashMap<String, String>,
) -> Result<String, String> {
    let arms = step
        .get("arms")
        .and_then(|a| a.as_array())
        .ok_or("select missing arms")?;
    if arms.len() != 2 {
        return Err("select requires exactly 2 arms".into());
    }
    let arm0 = PreparedArm::prepare(&arms[0], vars)?;
    let arm1 = PreparedArm::prepare(&arms[1], vars)?;
    let arm0_meta = arm0.clone();
    let arm1_meta = arm1.clone();

    let ctx0 = ctx.clone();
    let ctx1 = ctx.clone();
    let f0 = async move { execute_arm(&ctx0, arm0).await };
    let f1 = async move { execute_arm(&ctx1, arm1).await };

    let (payload, next_id, winner_idx) = match ctx.select2(f0, f1).await {
        Either2::First(r) => {
            let (p, n) = r?;
            (p, n, 0usize)
        }
        Either2::Second(r) => {
            let (p, n) = r?;
            (p, n, 1usize)
        }
    };

    if let Some(out_name) = step.get("out").and_then(|o| o.as_str()) {
        vars.insert(out_name.to_string(), payload.clone());
    }
    let winning = if winner_idx == 0 {
        &arm0_meta
    } else {
        &arm1_meta
    };
    if let Some(out_name) = winning.out_name() {
        vars.insert(out_name.to_string(), payload);
    }
    vars.insert("select_winner".into(), winner_idx.to_string());
    Ok(next_id)
}

async fn execute_arm(
    ctx: &OrchestrationContext,
    arm: PreparedArm,
) -> Result<(String, String), String> {
    match arm {
        PreparedArm::Timer {
            ms, value, next, ..
        } => {
            ctx.schedule_timer(Duration::from_millis(ms)).await;
            Ok((value, next))
        }
        PreparedArm::Wait { event, next, .. } => {
            let data = ctx.schedule_wait(event).await;
            Ok((data, next))
        }
        PreparedArm::Activity {
            name, input, next, ..
        } => {
            let out = ctx.schedule_activity(name, input).await?;
            Ok((out, next))
        }
    }
}

fn eval_cond(cond: Option<&JsonValue>, vars: &HashMap<String, String>) -> Result<bool, String> {
    let Some(cond) = cond else {
        return Err("if missing cond".into());
    };
    if let Some(s) = cond.as_str() {
        return Ok(is_truthy(&resolve_value(
            Some(&JsonValue::String(s.to_string())),
            vars,
        )));
    }
    if let Some(obj) = cond.as_object() {
        if let Some(arr) = obj.get("eq").and_then(|a| a.as_array()) {
            if arr.len() != 2 {
                return Err("cond.eq needs 2 elements".into());
            }
            let a = resolve_value(Some(&arr[0]), vars);
            let b = resolve_value(Some(&arr[1]), vars);
            return Ok(a == b);
        }
        if let Some(arr) = obj.get("neq").and_then(|a| a.as_array()) {
            if arr.len() != 2 {
                return Err("cond.neq needs 2 elements".into());
            }
            let a = resolve_value(Some(&arr[0]), vars);
            let b = resolve_value(Some(&arr[1]), vars);
            return Ok(a != b);
        }
        if let Some(t) = obj.get("truthy") {
            let v = resolve_value(Some(t), vars);
            return Ok(is_truthy(&v));
        }
    }
    Err("invalid cond expression".into())
}

fn is_truthy(s: &str) -> bool {
    let t = s.trim();
    !(t.is_empty()
        || t.eq_ignore_ascii_case("0")
        || t.eq_ignore_ascii_case("false")
        || t.eq_ignore_ascii_case("null")
        || t.eq_ignore_ascii_case("no"))
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

fn next_step(step: &JsonValue, current: &str) -> Result<String, String> {
    if let Some(n) = step.get("next").and_then(|x| x.as_str()) {
        return Ok(n.to_string());
    }
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

    #[test]
    fn truthy_rules() {
        assert!(is_truthy("yes"));
        assert!(is_truthy("1"));
        assert!(!is_truthy(""));
        assert!(!is_truthy("false"));
        assert!(!is_truthy("0"));
    }

    #[test]
    fn eval_eq() {
        let mut vars = HashMap::new();
        vars.insert("x".into(), "ok".into());
        let cond = serde_json::json!({"eq": ["$x", "ok"]});
        assert!(eval_cond(Some(&cond), &vars).unwrap());
        let cond = serde_json::json!({"neq": ["$x", "no"]});
        assert!(eval_cond(Some(&cond), &vars).unwrap());
    }
}

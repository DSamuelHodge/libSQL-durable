//! PVM Phase 4 — Process definitions as data.
//!
//! Process graphs live in the world (tables), not only in host Rust code.
//! The host supplies syscall implementations (activities); definitions name
//! which process/version an instance is pinned to and store a portable JSON
//! body (`pvm.def.v1` or host-defined schema).

use duroxide::providers::ProviderError;
use libsql::params;
use serde_json::Value as JsonValue;

use crate::native::NativeLibsqlProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessDefinition {
    pub name: String,
    pub version: String,
    /// Portable JSON body (graph / steps / metadata).
    pub body_json: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessDefinitionPin {
    pub instance_id: String,
    pub definition_name: String,
    pub definition_version: String,
    pub pinned_at_ms: i64,
}

/// Schema id for the portable step IR interpreted by the host.
pub const PVM_DEF_V1: &str = "pvm.def.v1";

/// Ops accepted by `pvm.def.v1` (additive; hosts ignore unknown only if schema not set).
pub const PVM_DEF_V1_OPS: &[&str] = &[
    "activity",
    "timer",
    "wait",
    "set_kv",
    "set_status",
    "return",
    "goto",
    "if",
    "select",
];

/// Validate `body_json`. Always requires valid JSON.
/// If `schema` is `"pvm.def.v1"`, enforce the step IR contract.
pub fn validate_definition_body(body_json: &str) -> Result<(), String> {
    let v: JsonValue =
        serde_json::from_str(body_json).map_err(|e| format!("body_json is not valid JSON: {e}"))?;
    let Some(obj) = v.as_object() else {
        return Ok(()); // non-object JSON allowed as opaque host body
    };
    let Some(schema) = obj.get("schema").and_then(|s| s.as_str()) else {
        return Ok(()); // no schema ⇒ opaque host-defined body
    };
    if schema != PVM_DEF_V1 {
        return Ok(()); // unknown schema: leave to host
    }
    // pvm.def.v1 required fields
    let entry = obj
        .get("entry")
        .and_then(|e| e.as_str())
        .ok_or_else(|| "pvm.def.v1 requires string field `entry`".to_string())?;
    let steps = obj
        .get("steps")
        .and_then(|s| s.as_array())
        .ok_or_else(|| "pvm.def.v1 requires array field `steps`".to_string())?;
    if steps.is_empty() {
        return Err("pvm.def.v1 `steps` must be non-empty".into());
    }
    let mut ids = std::collections::HashSet::new();
    for (i, step) in steps.iter().enumerate() {
        let s = step
            .as_object()
            .ok_or_else(|| format!("steps[{i}] must be an object"))?;
        let id = s
            .get("id")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("steps[{i}] requires string `id`"))?;
        if !ids.insert(id.to_string()) {
            return Err(format!("duplicate step id `{id}`"));
        }
        let op = s
            .get("op")
            .and_then(|x| x.as_str())
            .ok_or_else(|| format!("steps[{i}] requires string `op`"))?;
        match op {
            "activity" => {
                if s.get("name").and_then(|x| x.as_str()).is_none() {
                    return Err(format!("steps[{i}] activity requires `name`"));
                }
            }
            "timer" => {
                if s.get("ms").and_then(|x| x.as_u64()).is_none() {
                    return Err(format!("steps[{i}] timer requires numeric `ms`"));
                }
            }
            "wait" => {
                if s.get("event").and_then(|x| x.as_str()).is_none() {
                    return Err(format!("steps[{i}] wait requires `event`"));
                }
            }
            "set_kv" => {
                if s.get("key").and_then(|x| x.as_str()).is_none() {
                    return Err(format!("steps[{i}] set_kv requires `key`"));
                }
            }
            "set_status" | "return" | "goto" => {}
            "if" => {
                if s.get("cond").is_none() {
                    return Err(format!("steps[{i}] if requires `cond`"));
                }
                if s.get("then").and_then(|x| x.as_str()).is_none() {
                    return Err(format!("steps[{i}] if requires string `then`"));
                }
                // else optional — falls through to next if absent? require else for clarity
                if s.get("else").and_then(|x| x.as_str()).is_none() {
                    return Err(format!("steps[{i}] if requires string `else`"));
                }
                validate_cond(s.get("cond"), i)?;
            }
            "select" => {
                let arms = s
                    .get("arms")
                    .and_then(|a| a.as_array())
                    .ok_or_else(|| format!("steps[{i}] select requires array `arms`"))?;
                if arms.len() != 2 {
                    return Err(format!(
                        "steps[{i}] select requires exactly 2 arms (select2) in v1"
                    ));
                }
                for (j, arm) in arms.iter().enumerate() {
                    let a = arm
                        .as_object()
                        .ok_or_else(|| format!("steps[{i}].arms[{j}] must be object"))?;
                    let kind = a
                        .get("kind")
                        .and_then(|k| k.as_str())
                        .ok_or_else(|| format!("steps[{i}].arms[{j}] requires `kind`"))?;
                    match kind {
                        "timer" => {
                            if a.get("ms").and_then(|m| m.as_u64()).is_none() {
                                return Err(format!(
                                    "steps[{i}].arms[{j}] timer requires numeric `ms`"
                                ));
                            }
                        }
                        "wait" => {
                            if a.get("event").and_then(|e| e.as_str()).is_none() {
                                return Err(format!("steps[{i}].arms[{j}] wait requires `event`"));
                            }
                        }
                        "activity" => {
                            if a.get("name").and_then(|n| n.as_str()).is_none() {
                                return Err(format!(
                                    "steps[{i}].arms[{j}] activity requires `name`"
                                ));
                            }
                        }
                        other => {
                            return Err(format!(
                                "steps[{i}].arms[{j}] unknown kind `{other}` (timer|wait|activity)"
                            ));
                        }
                    }
                    if a.get("next").and_then(|n| n.as_str()).is_none() {
                        return Err(format!("steps[{i}].arms[{j}] requires string `next`"));
                    }
                }
            }
            other => {
                return Err(format!(
                    "steps[{i}] unknown op `{other}` (allowed: {})",
                    PVM_DEF_V1_OPS.join(", ")
                ));
            }
        }
    }
    if !ids.contains(entry) {
        return Err(format!("entry `{entry}` not found in steps"));
    }
    // Cross-check if/select targets exist
    for step in steps {
        let s = step.as_object().unwrap();
        let op = s.get("op").and_then(|o| o.as_str()).unwrap_or("");
        if op == "if" {
            for key in ["then", "else"] {
                if let Some(t) = s.get(key).and_then(|x| x.as_str())
                    && !ids.contains(t)
                {
                    return Err(format!("if `{key}` target `{t}` not found in steps"));
                }
            }
        }
        if op == "select"
            && let Some(arms) = s.get("arms").and_then(|a| a.as_array())
        {
            for arm in arms {
                if let Some(t) = arm.get("next").and_then(|n| n.as_str())
                    && !ids.contains(t)
                {
                    return Err(format!("select arm next `{t}` not found in steps"));
                }
            }
        }
        if op == "goto"
            && let Some(t) = s.get("target").and_then(|x| x.as_str())
            && !ids.contains(t)
        {
            return Err(format!("goto target `{t}` not found in steps"));
        }
        if let Some(n) = s.get("next").and_then(|x| x.as_str())
            && !ids.contains(n)
            && op != "return"
            && op != "if"
            && op != "select"
            && op != "goto"
        {
            return Err(format!("next target `{n}` not found in steps"));
        }
    }
    Ok(())
}

fn validate_cond(cond: Option<&JsonValue>, step_i: usize) -> Result<(), String> {
    let Some(cond) = cond else {
        return Err(format!("steps[{step_i}] if requires `cond`"));
    };
    if cond.is_string() {
        return Ok(());
    }
    if let Some(obj) = cond.as_object() {
        if obj.contains_key("eq") || obj.contains_key("neq") {
            let key = if obj.contains_key("eq") { "eq" } else { "neq" };
            let arr = obj
                .get(key)
                .and_then(|a| a.as_array())
                .ok_or_else(|| format!("steps[{step_i}] cond.{key} must be a 2-element array"))?;
            if arr.len() != 2 {
                return Err(format!("steps[{step_i}] cond.{key} must have length 2"));
            }
            return Ok(());
        }
        if obj.contains_key("truthy") {
            return Ok(());
        }
    }
    Err(format!(
        "steps[{step_i}] cond must be a string ($var), or {{eq|neq:[a,b]}}, or {{truthy:\"$var\"}}"
    ))
}

impl NativeLibsqlProvider {
    pub async fn ensure_definitions_schema(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ensure_definitions_schema", e))?;
        for sql in [
            r#"
            CREATE TABLE IF NOT EXISTS process_definitions (
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                body_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (name, version)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS process_definition_pins (
                instance_id TEXT PRIMARY KEY,
                definition_name TEXT NOT NULL,
                definition_version TEXT NOT NULL,
                pinned_at_ms INTEGER NOT NULL
            )
            "#,
            "CREATE INDEX IF NOT EXISTS idx_def_pins_name ON process_definition_pins(definition_name, definition_version)",
        ] {
            conn.execute(sql, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("ensure_definitions_schema", e))?;
        }
        Ok(())
    }

    /// Insert a process definition. Same `(name, version)` is **immutable** unless `force`.
    pub async fn put_process_definition(
        &self,
        name: &str,
        version: &str,
        body_json: &str,
    ) -> Result<(), ProviderError> {
        self.put_process_definition_ex(name, version, body_json, false)
            .await
    }

    /// Insert or (when `force`) replace a process definition.
    pub async fn put_process_definition_ex(
        &self,
        name: &str,
        version: &str,
        body_json: &str,
        force: bool,
    ) -> Result<(), ProviderError> {
        self.ensure_definitions_schema().await?;
        validate_definition_body(body_json)
            .map_err(|e| ProviderError::permanent("put_process_definition", e))?;

        if !force && self.get_process_definition(name, version).await?.is_some() {
            return Err(ProviderError::permanent(
                "put_process_definition",
                format!(
                    "definition {name}@{version} already exists (immutable; use force or a new version)"
                ),
            ));
        }

        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("put_process_definition", e))?;
        let now = Self::now_millis();
        if force {
            conn.execute(
                r#"
                INSERT INTO process_definitions (name, version, body_json, created_at_ms, updated_at_ms)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(name, version) DO UPDATE SET
                    body_json = excluded.body_json,
                    updated_at_ms = excluded.updated_at_ms
                "#,
                params![name, version, body_json, now, now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("put_process_definition", e))?;
        } else {
            conn.execute(
                r#"
                INSERT INTO process_definitions (name, version, body_json, created_at_ms, updated_at_ms)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![name, version, body_json, now, now],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("put_process_definition", e))?;
        }
        Ok(())
    }

    pub async fn get_process_definition(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Option<ProcessDefinition>, ProviderError> {
        self.ensure_definitions_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT name, version, body_json, created_at_ms, updated_at_ms
                FROM process_definitions WHERE name = ?1 AND version = ?2
                "#,
                params![name, version],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition", e))?;
        match rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition", e))?
        {
            Some(row) => Ok(Some(ProcessDefinition {
                name: row.get::<String>(0).unwrap_or_default(),
                version: row.get::<String>(1).unwrap_or_default(),
                body_json: row.get::<String>(2).unwrap_or_default(),
                created_at_ms: row.get::<i64>(3).unwrap_or(0),
                updated_at_ms: row.get::<i64>(4).unwrap_or(0),
            })),
            None => Ok(None),
        }
    }

    pub async fn list_process_definitions(&self) -> Result<Vec<ProcessDefinition>, ProviderError> {
        self.ensure_definitions_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_process_definitions", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT name, version, body_json, created_at_ms, updated_at_ms
                FROM process_definitions
                ORDER BY name, version
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_process_definitions", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_process_definitions", e))?
        {
            out.push(ProcessDefinition {
                name: row.get::<String>(0).unwrap_or_default(),
                version: row.get::<String>(1).unwrap_or_default(),
                body_json: row.get::<String>(2).unwrap_or_default(),
                created_at_ms: row.get::<i64>(3).unwrap_or(0),
                updated_at_ms: row.get::<i64>(4).unwrap_or(0),
            });
        }
        Ok(out)
    }

    pub async fn list_process_definitions_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<ProcessDefinition>, ProviderError> {
        let all = self.list_process_definitions().await?;
        Ok(all.into_iter().filter(|d| d.name == name).collect())
    }

    pub async fn delete_process_definition(
        &self,
        name: &str,
        version: &str,
    ) -> Result<bool, ProviderError> {
        self.ensure_definitions_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_process_definition", e))?;
        let n = conn
            .execute(
                "DELETE FROM process_definitions WHERE name = ?1 AND version = ?2",
                params![name, version],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("delete_process_definition", e))?;
        Ok(n > 0)
    }

    /// Pin an instance to a definition version (must already exist).
    pub async fn pin_process_definition(
        &self,
        instance_id: &str,
        definition_name: &str,
        definition_version: &str,
    ) -> Result<(), ProviderError> {
        self.ensure_definitions_schema().await?;
        if self
            .get_process_definition(definition_name, definition_version)
            .await?
            .is_none()
        {
            return Err(ProviderError::permanent(
                "pin_process_definition",
                format!("definition {definition_name}@{definition_version} not found"),
            ));
        }
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("pin_process_definition", e))?;
        let now = Self::now_millis();
        conn.execute(
            r#"
            INSERT INTO process_definition_pins
              (instance_id, definition_name, definition_version, pinned_at_ms)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(instance_id) DO UPDATE SET
                definition_name = excluded.definition_name,
                definition_version = excluded.definition_version,
                pinned_at_ms = excluded.pinned_at_ms
            "#,
            params![instance_id, definition_name, definition_version, now],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("pin_process_definition", e))?;
        Ok(())
    }

    pub async fn get_process_definition_pin(
        &self,
        instance_id: &str,
    ) -> Result<Option<ProcessDefinitionPin>, ProviderError> {
        self.ensure_definitions_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition_pin", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT instance_id, definition_name, definition_version, pinned_at_ms
                FROM process_definition_pins WHERE instance_id = ?1
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition_pin", e))?;
        match rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("get_process_definition_pin", e))?
        {
            Some(row) => Ok(Some(ProcessDefinitionPin {
                instance_id: row.get::<String>(0).unwrap_or_default(),
                definition_name: row.get::<String>(1).unwrap_or_default(),
                definition_version: row.get::<String>(2).unwrap_or_default(),
                pinned_at_ms: row.get::<i64>(3).unwrap_or(0),
            })),
            None => Ok(None),
        }
    }

    /// Resolve pin → definition for an instance.
    pub async fn resolve_definition_for_instance(
        &self,
        instance_id: &str,
    ) -> Result<Option<ProcessDefinition>, ProviderError> {
        let Some(pin) = self.get_process_definition_pin(instance_id).await? else {
            return Ok(None);
        };
        self.get_process_definition(&pin.definition_name, &pin.definition_version)
            .await
    }
}

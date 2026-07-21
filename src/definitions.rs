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
            other => {
                return Err(format!(
                    "steps[{i}] unknown op `{other}` (allowed: activity, timer, wait, set_kv, set_status, return, goto)"
                ));
            }
        }
    }
    if !ids.contains(entry) {
        return Err(format!("entry `{entry}` not found in steps"));
    }
    Ok(())
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
        validate_definition_body(body_json).map_err(|e| {
            ProviderError::permanent("put_process_definition", e)
        })?;

        if !force {
            if self.get_process_definition(name, version).await?.is_some() {
                return Err(ProviderError::permanent(
                    "put_process_definition",
                    format!(
                        "definition {name}@{version} already exists (immutable; use force or a new version)"
                    ),
                ));
            }
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

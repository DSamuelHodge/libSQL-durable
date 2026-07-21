//! PVM Phase 4 — Process definitions as data.
//!
//! Process graphs live in the world (tables), not only in host Rust code.
//! The host still supplies syscall implementations (activities); definitions
//! name which process/version an instance is pinned to and store a portable
//! JSON body for inspection, migration, and future interpreters.

use duroxide::providers::ProviderError;
use libsql::params;
use serde_json::Value as JsonValue;

use crate::native::NativeLibsqlProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessDefinition {
    pub name: String,
    pub version: String,
    /// Portable JSON body (graph / steps / metadata). Host-defined schema.
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

    /// Upsert a process definition (name, version) → JSON body.
    pub async fn put_process_definition(
        &self,
        name: &str,
        version: &str,
        body_json: &str,
    ) -> Result<(), ProviderError> {
        self.ensure_definitions_schema().await?;
        // Validate JSON so garbage does not enter the world.
        let _: JsonValue = serde_json::from_str(body_json).map_err(|e| {
            ProviderError::permanent(
                "put_process_definition",
                format!("body_json is not valid JSON: {e}"),
            )
        })?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("put_process_definition", e))?;
        let now = Self::now_millis();
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

    /// Pin an instance to a definition version (must already exist unless force).
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
}

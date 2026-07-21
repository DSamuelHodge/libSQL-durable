//! PVM Phase 1 — World packaging.
//!
//! A **world** is the portable durable computer: one libSQL database file (plus
//! optional WAL/SHM sidecars) or a remote topology that holds the same kernel
//! schema. This module defines the manifest, version fence, and local copy/
//! resume helpers.
//!
//! See [`crate::docs`] is not a module — see repository `docs/PVM.md` and
//! `docs/WORLD_PACKAGE.md`.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use libsql::params;

use crate::LibsqlProviderInitError;

/// Kernel / orchestration schema revision applied by migrate.
/// Bump when making backward-compatible DDL additions.
///
/// - 1: base durable kernel + world_manifest
/// - 2: definitions, fork metadata, adaptive policy, world mesh tables
pub const SCHEMA_VERSION: i64 = 2;

/// World packaging format version (independent of orchestration schema version).
pub const WORLD_FORMAT_VERSION: i64 = 2;

/// Oldest schema version this host will open (after migrate).
pub const MIN_COMPATIBLE_SCHEMA_VERSION: i64 = 1;

/// Stable identity and version fence for a PVM world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldManifest {
    pub world_id: String,
    pub schema_version: i64,
    pub world_format_version: i64,
    pub provider_name: String,
    pub provider_version: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Semver lower bound of hosts intended to open this world (informational).
    pub runtime_semver_min: String,
    /// Semver upper bound (informational; hard fence uses schema_version).
    pub runtime_semver_max: String,
    /// If this world was forked, parent world_id (Phase 5).
    pub parent_world_id: Option<String>,
    /// Optional human/system note about the fork.
    pub fork_note: Option<String>,
}

/// Result of opening / validating a world (checklist snapshot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldOpenReport {
    pub manifest: WorldManifest,
    pub schema_ok: bool,
    pub format_ok: bool,
    pub provider_name: String,
    pub provider_version: String,
    pub notes: Vec<String>,
}

impl WorldOpenReport {
    pub fn is_ok(&self) -> bool {
        self.schema_ok && self.format_ok
    }
}

/// Files that constitute a local world package on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldPackagePaths {
    pub db: PathBuf,
    pub wal: PathBuf,
    pub shm: PathBuf,
}

impl WorldPackagePaths {
    pub fn for_db(db_path: impl Into<PathBuf>) -> Self {
        let db = db_path.into();
        let wal = PathBuf::from(format!("{}-wal", db.display()));
        let shm = PathBuf::from(format!("{}-shm", db.display()));
        Self { db, wal, shm }
    }

    pub fn existing_sidecars(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if self.wal.exists() {
            out.push(self.wal.clone());
        }
        if self.shm.exists() {
            out.push(self.shm.clone());
        }
        out
    }
}

/// Generate a new world id (no external uuid dependency).
pub fn new_world_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("world-{}-{}", std::process::id(), nanos)
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Hard version fence: refuse worlds from the future; allow older that we can migrate.
pub fn check_schema_fence(found: i64) -> Result<(), LibsqlProviderInitError> {
    if found > SCHEMA_VERSION {
        return Err(LibsqlProviderInitError::WorldIncompatible(format!(
            "world schema version {found} is newer than this host supports ({SCHEMA_VERSION}); upgrade the host binary"
        )));
    }
    if found < MIN_COMPATIBLE_SCHEMA_VERSION {
        return Err(LibsqlProviderInitError::WorldIncompatible(format!(
            "world schema version {found} is older than minimum supported ({MIN_COMPATIBLE_SCHEMA_VERSION})"
        )));
    }
    Ok(())
}

pub fn check_format_fence(found: i64) -> Result<(), LibsqlProviderInitError> {
    if found > WORLD_FORMAT_VERSION {
        return Err(LibsqlProviderInitError::WorldIncompatible(format!(
            "world format version {found} is newer than this host supports ({WORLD_FORMAT_VERSION})"
        )));
    }
    if found < 1 {
        return Err(LibsqlProviderInitError::WorldIncompatible(format!(
            "invalid world format version {found}"
        )));
    }
    Ok(())
}

/// Copy a **quiesced** local world to a new path (db + optional -wal/-shm).
///
/// # Safety contract
///
/// - Do **not** copy a live multi-writer world without checkpointing first.
/// - Prefer [`crate::NativeLibsqlProvider::checkpoint_wal`] then copy, or stop all hosts.
/// - Destination parent directories are created as needed.
pub fn copy_world_package(
    src_db: impl AsRef<Path>,
    dst_db: impl AsRef<Path>,
) -> Result<WorldPackagePaths, LibsqlProviderInitError> {
    let src = WorldPackagePaths::for_db(src_db.as_ref());
    let dst = WorldPackagePaths::for_db(dst_db.as_ref());

    if !src.db.exists() {
        return Err(LibsqlProviderInitError::InvalidConfig(format!(
            "source world db does not exist: {}",
            src.db.display()
        )));
    }

    if let Some(parent) = dst.db.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            LibsqlProviderInitError::InvalidConfig(format!(
                "failed to create destination directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    fs::copy(&src.db, &dst.db).map_err(|e| {
        LibsqlProviderInitError::InvalidConfig(format!(
            "failed to copy {} -> {}: {e}",
            src.db.display(),
            dst.db.display()
        ))
    })?;

    if src.wal.exists() {
        fs::copy(&src.wal, &dst.wal).map_err(|e| {
            LibsqlProviderInitError::InvalidConfig(format!(
                "failed to copy WAL {} -> {}: {e}",
                src.wal.display(),
                dst.wal.display()
            ))
        })?;
    } else if dst.wal.exists() {
        let _ = fs::remove_file(&dst.wal);
    }

    if src.shm.exists() {
        fs::copy(&src.shm, &dst.shm).map_err(|e| {
            LibsqlProviderInitError::InvalidConfig(format!(
                "failed to copy SHM {} -> {}: {e}",
                src.shm.display(),
                dst.shm.display()
            ))
        })?;
    } else if dst.shm.exists() {
        let _ = fs::remove_file(&dst.shm);
    }

    Ok(dst)
}

/// Open checklist used by hosts (and docs) when resuming a world.
pub fn open_world_checklist() -> &'static [&'static str] {
    &[
        "1. Confirm no live writer is mid-transaction (stop host or checkpoint WAL).",
        "2. Open with a host binary whose SCHEMA_VERSION >= world schema (migrate) and <= world only if equal-or-host-newer.",
        "3. Read world_manifest: world_id, schema_version, world_format_version.",
        "4. Refuse if world schema/format is newer than this host (upgrade host).",
        "5. Run migrate() / create_schema to apply additive DDL and refresh manifest timestamps.",
        "6. Verify provider name is libsql-native (or compatible) before scheduling work.",
        "7. For copy/resume: copy db (+ wal/shm if present) then open destination path only.",
    ]
}

/// Ensure `world_manifest` exists and is fenced against this host binary.
pub async fn ensure_world_manifest(
    conn: &libsql::Connection,
    provider_name: &str,
    provider_version: &str,
) -> Result<WorldManifest, LibsqlProviderInitError> {
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS world_manifest (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            world_id TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            world_format_version INTEGER NOT NULL,
            provider_name TEXT NOT NULL,
            provider_version TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            runtime_semver_min TEXT NOT NULL,
            runtime_semver_max TEXT NOT NULL,
            parent_world_id TEXT,
            fork_note TEXT
        )
        "#,
        (),
    )
    .await?;
    // Additive columns for worlds created on format v1.
    let _ = conn
        .execute(
            "ALTER TABLE world_manifest ADD COLUMN parent_world_id TEXT",
            (),
        )
        .await;
    let _ = conn
        .execute("ALTER TABLE world_manifest ADD COLUMN fork_note TEXT", ())
        .await;

    // Fence against a future schema already recorded in schema_meta (if any).
    if let Some(found) = read_schema_meta_version(conn).await? {
        check_schema_fence(found)?;
    }

    let mut rows = conn
        .query(
            r#"
            SELECT world_id, schema_version, world_format_version, provider_name, provider_version,
                   created_at_ms, updated_at_ms, runtime_semver_min, runtime_semver_max,
                   parent_world_id, fork_note
            FROM world_manifest WHERE id = 1
            "#,
            (),
        )
        .await?;

    let now = now_millis();
    if let Some(row) = rows.next().await? {
        let format_v = row.get::<i64>(2)?;
        check_format_fence(format_v)?;
        let schema_v = row.get::<i64>(1)?;
        check_schema_fence(schema_v)?;

        let parent_world_id = optional_text(row.get_value(9).ok());
        let fork_note = optional_text(row.get_value(10).ok());

        // Refresh host stamp; advance schema_version to this host after migrate.
        conn.execute(
            r#"
            UPDATE world_manifest SET
                schema_version = ?1,
                world_format_version = ?2,
                provider_name = ?3,
                provider_version = ?4,
                updated_at_ms = ?5,
                runtime_semver_max = ?6
            WHERE id = 1
            "#,
            params![
                SCHEMA_VERSION,
                WORLD_FORMAT_VERSION,
                provider_name,
                provider_version,
                now,
                env!("CARGO_PKG_VERSION")
            ],
        )
        .await?;

        return Ok(WorldManifest {
            world_id: row.get::<String>(0)?,
            schema_version: SCHEMA_VERSION,
            world_format_version: WORLD_FORMAT_VERSION,
            provider_name: provider_name.to_string(),
            provider_version: provider_version.to_string(),
            created_at_ms: row.get::<i64>(5)?,
            updated_at_ms: now,
            runtime_semver_min: row.get::<String>(7)?,
            runtime_semver_max: env!("CARGO_PKG_VERSION").to_string(),
            parent_world_id,
            fork_note,
        });
    }

    let world_id = new_world_id();
    let ver = env!("CARGO_PKG_VERSION");
    conn.execute(
        r#"
        INSERT INTO world_manifest (
            id, world_id, schema_version, world_format_version,
            provider_name, provider_version, created_at_ms, updated_at_ms,
            runtime_semver_min, runtime_semver_max, parent_world_id, fork_note
        ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL)
        "#,
        params![
            world_id.as_str(),
            SCHEMA_VERSION,
            WORLD_FORMAT_VERSION,
            provider_name,
            provider_version,
            now,
            now,
            ver,
            ver
        ],
    )
    .await?;

    Ok(WorldManifest {
        world_id,
        schema_version: SCHEMA_VERSION,
        world_format_version: WORLD_FORMAT_VERSION,
        provider_name: provider_name.to_string(),
        provider_version: provider_version.to_string(),
        created_at_ms: now,
        updated_at_ms: now,
        runtime_semver_min: ver.to_string(),
        runtime_semver_max: ver.to_string(),
        parent_world_id: None,
        fork_note: None,
    })
}

fn optional_text(value: Option<libsql::Value>) -> Option<String> {
    match value {
        Some(libsql::Value::Text(s)) => Some(s),
        _ => None,
    }
}

pub async fn read_world_manifest(
    conn: &libsql::Connection,
) -> Result<Option<WorldManifest>, LibsqlProviderInitError> {
    let rows = conn
        .query(
            r#"
            SELECT world_id, schema_version, world_format_version, provider_name, provider_version,
                   created_at_ms, updated_at_ms, runtime_semver_min, runtime_semver_max,
                   parent_world_id, fork_note
            FROM world_manifest WHERE id = 1
            "#,
            (),
        )
        .await;
    let mut rows = match rows {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("no such table") {
                return Ok(None);
            }
            return Err(e.into());
        }
    };
    match rows.next().await? {
        Some(row) => Ok(Some(WorldManifest {
            world_id: row.get::<String>(0)?,
            schema_version: row.get::<i64>(1)?,
            world_format_version: row.get::<i64>(2)?,
            provider_name: row.get::<String>(3)?,
            provider_version: row.get::<String>(4)?,
            created_at_ms: row.get::<i64>(5)?,
            updated_at_ms: row.get::<i64>(6)?,
            runtime_semver_min: row.get::<String>(7)?,
            runtime_semver_max: row.get::<String>(8)?,
            parent_world_id: optional_text(row.get_value(9).ok()),
            fork_note: optional_text(row.get_value(10).ok()),
        })),
        None => Ok(None),
    }
}

/// Stamp fork lineage on an already-copied world (Phase 5).
pub async fn stamp_fork_lineage(
    conn: &libsql::Connection,
    parent_world_id: &str,
    fork_note: Option<&str>,
) -> Result<(), LibsqlProviderInitError> {
    let now = now_millis();
    let new_id = new_world_id();
    conn.execute(
        r#"
        UPDATE world_manifest SET
            world_id = ?1,
            parent_world_id = ?2,
            fork_note = ?3,
            updated_at_ms = ?4
        WHERE id = 1
        "#,
        params![new_id, parent_world_id, fork_note.unwrap_or("fork"), now],
    )
    .await?;
    Ok(())
}

async fn read_schema_meta_version(
    conn: &libsql::Connection,
) -> Result<Option<i64>, LibsqlProviderInitError> {
    let mut rows = match conn
        .query("SELECT version FROM schema_meta WHERE id = 1", ())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("no such table") {
                return Ok(None);
            }
            return Err(e.into());
        }
    };
    match rows.next().await? {
        Some(row) => Ok(Some(row.get::<i64>(0)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_rejects_future_schema() {
        let err = check_schema_fence(SCHEMA_VERSION + 10).unwrap_err();
        assert!(matches!(err, LibsqlProviderInitError::WorldIncompatible(_)));
    }

    #[test]
    fn fence_accepts_current() {
        check_schema_fence(SCHEMA_VERSION).unwrap();
        check_schema_fence(MIN_COMPATIBLE_SCHEMA_VERSION).unwrap();
    }

    #[test]
    fn package_paths_sidecars() {
        let p = WorldPackagePaths::for_db("/tmp/world.db");
        assert_eq!(p.wal, PathBuf::from("/tmp/world.db-wal"));
        assert_eq!(p.shm, PathBuf::from("/tmp/world.db-shm"));
    }
}

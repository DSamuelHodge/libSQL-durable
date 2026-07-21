//! PVM Phase 5 — World fork and time travel.
//!
//! Fork = world-grain subprocess: copy the durable computer, optionally trim
//! journals past a point, clear scheduler state for clean explore, stamp lineage.

use std::path::{Path, PathBuf};

use duroxide::providers::ProviderError;
use libsql::params;

use crate::native::NativeLibsqlProvider;
use crate::world::{self, WorldPackagePaths};
use crate::{copy_world_package, LibsqlDatabaseConfig, LibsqlProvider, LibsqlProviderInitError};

#[derive(Debug, Clone, Default)]
pub struct ForkOptions {
    /// Human/system note stored on the child world.
    pub note: Option<String>,
    /// If set, delete history events with event_id > this for all instances (time travel cut).
    pub truncate_after_event_id: Option<u64>,
    /// If set, only keep history for this instance (others cleared).
    pub keep_instance: Option<String>,
    /// Clear queues and locks on the fork so exploration starts without stale locks.
    pub clear_scheduler_state: bool,
}

impl ForkOptions {
    /// Default explore sandbox: clear scheduler state, note `"explore"`.
    pub fn explore() -> Self {
        Self {
            note: Some("explore".into()),
            clear_scheduler_state: true,
            ..Default::default()
        }
    }

    /// Explore focused on one instance (retain only that instance's rows).
    pub fn explore_instance(instance_id: impl Into<String>) -> Self {
        Self {
            note: Some("explore".into()),
            keep_instance: Some(instance_id.into()),
            clear_scheduler_state: true,
            ..Default::default()
        }
    }

    /// Time-travel cut: keep events with `event_id <= after`, clear scheduler.
    pub fn time_travel(after_event_id: u64) -> Self {
        Self {
            note: Some("time-travel".into()),
            truncate_after_event_id: Some(after_event_id),
            clear_scheduler_state: true,
            ..Default::default()
        }
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkResult {
    pub parent_world_id: String,
    pub child_world_id: String,
    pub child_path: PathBuf,
    pub note: Option<String>,
}

impl NativeLibsqlProvider {
    /// Fork this **file-backed** world to `dst_db` as a world-grain subprocess.
    ///
    /// Steps: checkpoint → copy package → open child → stamp new world_id + parent
    /// lineage → optional history truncate / scheduler clear.
    pub async fn fork_world_to(
        &self,
        src_db: impl AsRef<Path>,
        dst_db: impl AsRef<Path>,
        options: ForkOptions,
    ) -> Result<ForkResult, LibsqlProviderInitError> {
        let parent = self
            .world_manifest()
            .await?
            .ok_or_else(|| {
                LibsqlProviderInitError::InvalidConfig("parent world_manifest missing".into())
            })?;

        let _ = self.checkpoint_wal().await;
        copy_world_package(src_db.as_ref(), dst_db.as_ref())?;

        // Open child and re-stamp identity so it is a distinct computer.
        let child = LibsqlProvider::new(LibsqlDatabaseConfig::local(dst_db.as_ref())).await?;
        let native = child.native().ok_or_else(|| {
            LibsqlProviderInitError::InvalidConfig("child is not native backend".into())
        })?;
        let conn = native.connect().await?;
        world::stamp_fork_lineage(
            &conn,
            &parent.world_id,
            options.note.as_deref(),
        )
        .await?;
        drop(conn);

        if let Some(cut) = options.truncate_after_event_id {
            native
                .time_travel_truncate(cut, options.keep_instance.as_deref())
                .await
                .map_err(|e| {
                    LibsqlProviderInitError::InvalidConfig(format!("time travel truncate: {e}"))
                })?;
        } else if let Some(ref only) = options.keep_instance {
            // Drop other instances' history for a focused explore world.
            native
                .retain_instance_only(only)
                .await
                .map_err(|e| {
                    LibsqlProviderInitError::InvalidConfig(format!("retain instance: {e}"))
                })?;
        }

        if options.clear_scheduler_state {
            native.clear_scheduler_state().await.map_err(|e| {
                LibsqlProviderInitError::InvalidConfig(format!("clear scheduler: {e}"))
            })?;
        }

        let child_manifest = child.world_manifest().await?.ok_or_else(|| {
            LibsqlProviderInitError::InvalidConfig("child manifest missing after fork".into())
        })?;

        Ok(ForkResult {
            parent_world_id: parent.world_id,
            child_world_id: child_manifest.world_id,
            child_path: PathBuf::from(dst_db.as_ref()),
            note: options.note,
        })
    }

    /// Delete history events with event_id greater than `after_event_id`.
    pub async fn time_travel_truncate(
        &self,
        after_event_id: u64,
        only_instance: Option<&str>,
    ) -> Result<u64, ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("time_travel_truncate", e))?;
        let cut = after_event_id as i64;
        let n = if let Some(instance) = only_instance {
            conn.execute(
                "DELETE FROM history WHERE instance_id = ?1 AND event_id > ?2",
                params![instance, cut],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("time_travel_truncate", e))?
        } else {
            conn.execute("DELETE FROM history WHERE event_id > ?1", params![cut])
                .await
                .map_err(|e| Self::libsql_to_provider_error("time_travel_truncate", e))?
        };
        Ok(n)
    }

    /// Keep one instance's rows; delete others (explore sandbox).
    pub async fn retain_instance_only(&self, instance_id: &str) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("retain_instance_only", e))?;
        for sql in [
            "DELETE FROM history WHERE instance_id != ?1",
            "DELETE FROM executions WHERE instance_id != ?1",
            "DELETE FROM instances WHERE instance_id != ?1",
            "DELETE FROM kv_store WHERE instance_id != ?1",
            "DELETE FROM kv_delta WHERE instance_id != ?1",
            "DELETE FROM orchestrator_queue WHERE instance_id IS NOT NULL AND instance_id != ?1",
            "DELETE FROM worker_queue WHERE instance_id IS NOT NULL AND instance_id != ?1",
            "DELETE FROM instance_locks WHERE instance_id != ?1",
            // Best-effort: pins table may not exist on ancient worlds.
            "DELETE FROM process_definition_pins WHERE instance_id != ?1",
        ] {
            let _ = conn.execute(sql, params![instance_id]).await;
        }
        Ok(())
    }

    /// Fork and open the child as a live provider (explore convenience).
    pub async fn fork_and_open(
        &self,
        src_db: impl AsRef<Path>,
        dst_db: impl AsRef<Path>,
        options: ForkOptions,
    ) -> Result<(ForkResult, LibsqlProvider), LibsqlProviderInitError> {
        let result = self.fork_world_to(src_db, &dst_db, options).await?;
        let child = LibsqlProvider::new(LibsqlDatabaseConfig::local(dst_db.as_ref())).await?;
        Ok((result, child))
    }

    /// Clear queues and locks for a clean explore fork.
    pub async fn clear_scheduler_state(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("clear_scheduler_state", e))?;
        for sql in [
            "DELETE FROM orchestrator_queue",
            "DELETE FROM worker_queue",
            "DELETE FROM instance_locks",
            "DELETE FROM sessions",
        ] {
            conn.execute(sql, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("clear_scheduler_state", e))?;
        }
        Ok(())
    }
}

/// Filesystem-only fork helper when you do not hold a live provider on src.
pub fn fork_world_files(
    src_db: impl AsRef<Path>,
    dst_db: impl AsRef<Path>,
) -> Result<WorldPackagePaths, LibsqlProviderInitError> {
    copy_world_package(src_db, dst_db)
}

/// Delete a local world package (db + wal + shm). Refuses if `path` does not exist as a file.
pub fn discard_world_package(path: impl AsRef<Path>) -> Result<(), LibsqlProviderInitError> {
    let paths = WorldPackagePaths::for_db(path.as_ref());
    if !paths.db.exists() {
        return Err(LibsqlProviderInitError::InvalidConfig(format!(
            "discard: world file not found: {}",
            paths.db.display()
        )));
    }
    for p in [&paths.db, &paths.wal, &paths.shm] {
        if p.exists() {
            std::fs::remove_file(p).map_err(|e| {
                LibsqlProviderInitError::InvalidConfig(format!(
                    "discard {}: {e}",
                    p.display()
                ))
            })?;
        }
    }
    Ok(())
}

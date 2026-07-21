//! PVM Phase 5 — World fork and time travel.
//!
//! Fork = world-grain subprocess: copy the durable computer, optionally trim
//! journals past a point, clear scheduler state for clean explore, stamp lineage.

use std::path::{Path, PathBuf};

use duroxide::providers::ProviderError;
use libsql::params;

use crate::native::NativeLibsqlProvider;
use crate::world::{self, WorldPackagePaths};
use crate::{LibsqlDatabaseConfig, LibsqlProvider, LibsqlProviderInitError, copy_world_package};

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
        let parent = self.world_manifest().await?.ok_or_else(|| {
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
        world::stamp_fork_lineage(&conn, &parent.world_id, options.note.as_deref()).await?;
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
            native.retain_instance_only(only).await.map_err(|e| {
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
    remove_package_files(&paths)?;
    Ok(())
}

/// Options for file-level promote (replace parent package with child).
#[derive(Debug, Clone)]
pub struct PromoteOptions {
    /// Must be `true` — refuse otherwise (no silent overwrite).
    pub confirm: bool,
    /// Require `child.parent_world_id == parent.world_id` (default true).
    pub require_lineage: bool,
    /// Optional directory for parent backup; default next to parent as `*.promote-bak-<ts>`.
    pub backup_dir: Option<PathBuf>,
    /// Delete child package after successful promote.
    pub discard_child: bool,
    /// Note stored in promote audit on the promoted world.
    pub note: Option<String>,
}

impl Default for PromoteOptions {
    fn default() -> Self {
        Self {
            confirm: false,
            require_lineage: true,
            backup_dir: None,
            discard_child: false,
            note: None,
        }
    }
}

impl PromoteOptions {
    /// Explicit confirm required for any promote.
    pub fn confirmed() -> Self {
        Self {
            confirm: true,
            ..Default::default()
        }
    }

    pub fn with_discard_child(mut self, discard: bool) -> Self {
        self.discard_child = discard;
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    pub fn without_lineage_check(mut self) -> Self {
        self.require_lineage = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromoteResult {
    pub parent_path: PathBuf,
    pub backup_path: PathBuf,
    pub child_path: PathBuf,
    pub previous_parent_world_id: String,
    pub promoted_world_id: String,
    pub discarded_child: bool,
}

/// Promote child world **file** over parent (policy A).
///
/// Safety:
/// - `options.confirm` must be true
/// - parent ≠ child path
/// - by default child must declare `parent_world_id` equal to parent's `world_id`
/// - parent package is copied to a backup path first
/// - then parent package is replaced with a copy of the child package
///
/// Callers must **not** hold open writers on parent/child packages.
pub async fn promote_world_package(
    parent_db: impl AsRef<Path>,
    child_db: impl AsRef<Path>,
    options: PromoteOptions,
) -> Result<PromoteResult, LibsqlProviderInitError> {
    if !options.confirm {
        return Err(LibsqlProviderInitError::InvalidConfig(
            "promote refused: set PromoteOptions.confirm = true (explicit acknowledgement required)"
                .into(),
        ));
    }

    let parent_path = parent_db.as_ref().to_path_buf();
    let child_path = child_db.as_ref().to_path_buf();
    if same_path(&parent_path, &child_path) {
        return Err(LibsqlProviderInitError::InvalidConfig(
            "promote refused: parent and child paths must differ".into(),
        ));
    }
    if !parent_path.exists() {
        return Err(LibsqlProviderInitError::InvalidConfig(format!(
            "promote: parent world not found: {}",
            parent_path.display()
        )));
    }
    if !child_path.exists() {
        return Err(LibsqlProviderInitError::InvalidConfig(format!(
            "promote: child world not found: {}",
            child_path.display()
        )));
    }

    // Read lineage while packages are intact.
    let parent = LibsqlProvider::new(LibsqlDatabaseConfig::local(&parent_path)).await?;
    let child = LibsqlProvider::new(LibsqlDatabaseConfig::local(&child_path)).await?;
    let parent_manifest = parent.world_manifest().await?.ok_or_else(|| {
        LibsqlProviderInitError::InvalidConfig("parent world_manifest missing".into())
    })?;
    let child_manifest = child.world_manifest().await?.ok_or_else(|| {
        LibsqlProviderInitError::InvalidConfig("child world_manifest missing".into())
    })?;

    if options.require_lineage {
        match &child_manifest.parent_world_id {
            Some(pid) if pid == &parent_manifest.world_id => {}
            Some(pid) => {
                return Err(LibsqlProviderInitError::InvalidConfig(format!(
                    "promote lineage mismatch: child.parent_world_id={pid} parent.world_id={}",
                    parent_manifest.world_id
                )));
            }
            None => {
                return Err(LibsqlProviderInitError::InvalidConfig(
                    "promote refused: child has no parent_world_id (not a fork of this parent); use without_lineage_check only if intentional"
                        .into(),
                ));
            }
        }
    }

    let _ = parent.checkpoint_wal().await;
    let _ = child.checkpoint_wal().await;
    drop(parent);
    drop(child);

    let ts = world::now_millis();
    let backup_path = match &options.backup_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).map_err(|e| {
                LibsqlProviderInitError::InvalidConfig(format!(
                    "promote backup_dir {}: {e}",
                    dir.display()
                ))
            })?;
            dir.join(format!(
                "{}.promote-bak-{ts}",
                parent_path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("world.db")
            ))
        }
        None => PathBuf::from(format!("{}-promote-bak-{ts}", parent_path.display())),
    };

    // 1) Backup parent package
    copy_world_package(&parent_path, &backup_path)?;

    // 2) Remove parent package files so copy is clean
    let parent_pkg = WorldPackagePaths::for_db(&parent_path);
    remove_package_files(&parent_pkg)?;

    // 3) Install child package at parent path
    copy_world_package(&child_path, &parent_path)?;

    // 4) Audit on promoted world (now at parent_path)
    let promoted = LibsqlProvider::new(LibsqlDatabaseConfig::local(&parent_path)).await?;
    if let Some(native) = promoted.native() {
        let _ = native
            .record_promote_audit(
                &parent_manifest.world_id,
                &child_manifest.world_id,
                &backup_path.display().to_string(),
                options.note.as_deref(),
            )
            .await;
    }
    let promoted_id = promoted
        .world_manifest()
        .await?
        .map(|m| m.world_id)
        .unwrap_or_else(|| child_manifest.world_id.clone());
    drop(promoted);

    let mut discarded_child = false;
    if options.discard_child {
        discard_world_package(&child_path)?;
        discarded_child = true;
    }

    Ok(PromoteResult {
        parent_path,
        backup_path,
        child_path,
        previous_parent_world_id: parent_manifest.world_id,
        promoted_world_id: promoted_id,
        discarded_child,
    })
}

fn same_path(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => false,
    }
}

fn remove_package_files(paths: &WorldPackagePaths) -> Result<(), LibsqlProviderInitError> {
    for p in [&paths.db, &paths.wal, &paths.shm] {
        if p.exists() {
            std::fs::remove_file(p).map_err(|e| {
                LibsqlProviderInitError::InvalidConfig(format!("remove {}: {e}", p.display()))
            })?;
        }
    }
    Ok(())
}

/// Policy B: import selected instances from a forked child into a **live parent**.
///
/// Parent `world_id` is preserved. For each listed instance, parent rows are
/// replaced with the child's history / executions / instance row (and optionally
/// KV + definition pins). Scheduler rows for those instances are cleared on parent.
#[derive(Debug, Clone)]
pub struct SelectivePromoteOptions {
    pub confirm: bool,
    pub require_lineage: bool,
    /// Instance ids to import from child (must be non-empty).
    pub instance_ids: Vec<String>,
    pub include_kv: bool,
    pub include_definition_pins: bool,
    pub note: Option<String>,
}

impl Default for SelectivePromoteOptions {
    fn default() -> Self {
        Self {
            confirm: false,
            require_lineage: true,
            instance_ids: Vec::new(),
            include_kv: true,
            include_definition_pins: true,
            note: None,
        }
    }
}

impl SelectivePromoteOptions {
    pub fn confirmed(instance_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            confirm: true,
            instance_ids: instance_ids.into_iter().map(|s| s.into()).collect(),
            ..Default::default()
        }
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectivePromoteResult {
    pub parent_world_id: String,
    pub child_world_id: String,
    pub imported_instances: Vec<String>,
    pub history_events_imported: u64,
}

/// Import instance state from child world into parent (policy B).
///
/// Requires open providers. Prefer quiescing both worlds (no active dispatchers).
pub async fn selective_promote_instances(
    parent: &LibsqlProvider,
    child: &LibsqlProvider,
    options: SelectivePromoteOptions,
) -> Result<SelectivePromoteResult, LibsqlProviderInitError> {
    if !options.confirm {
        return Err(LibsqlProviderInitError::InvalidConfig(
            "selective promote refused: confirm must be true".into(),
        ));
    }
    if options.instance_ids.is_empty() {
        return Err(LibsqlProviderInitError::InvalidConfig(
            "selective promote refused: instance_ids empty".into(),
        ));
    }

    let parent_native = parent.native().ok_or_else(|| {
        LibsqlProviderInitError::InvalidConfig("parent not native backend".into())
    })?;
    let child_native = child
        .native()
        .ok_or_else(|| LibsqlProviderInitError::InvalidConfig("child not native backend".into()))?;

    let parent_manifest = parent.world_manifest().await?.ok_or_else(|| {
        LibsqlProviderInitError::InvalidConfig("parent world_manifest missing".into())
    })?;
    let child_manifest = child.world_manifest().await?.ok_or_else(|| {
        LibsqlProviderInitError::InvalidConfig("child world_manifest missing".into())
    })?;

    if options.require_lineage {
        match &child_manifest.parent_world_id {
            Some(pid) if pid == &parent_manifest.world_id => {}
            Some(pid) => {
                return Err(LibsqlProviderInitError::InvalidConfig(format!(
                    "selective promote lineage mismatch: child.parent_world_id={pid} parent={}",
                    parent_manifest.world_id
                )));
            }
            None => {
                return Err(LibsqlProviderInitError::InvalidConfig(
                    "selective promote refused: child has no parent_world_id".into(),
                ));
            }
        }
    }

    let mut history_events_imported = 0u64;
    let mut imported = Vec::new();

    for instance_id in &options.instance_ids {
        let n = parent_native
            .import_instance_from(
                child_native,
                instance_id,
                options.include_kv,
                options.include_definition_pins,
            )
            .await
            .map_err(|e| {
                LibsqlProviderInitError::InvalidConfig(format!(
                    "import instance {instance_id}: {e}"
                ))
            })?;
        history_events_imported += n;
        imported.push(instance_id.clone());
    }

    let note = format!(
        "selective instances={} hist={} {}",
        imported.join(","),
        history_events_imported,
        options.note.as_deref().unwrap_or("")
    );
    let _ = parent_native
        .record_promote_audit(
            &parent_manifest.world_id,
            &child_manifest.world_id,
            "(selective-in-place)",
            Some(&note),
        )
        .await;

    Ok(SelectivePromoteResult {
        parent_world_id: parent_manifest.world_id,
        child_world_id: child_manifest.world_id,
        imported_instances: imported,
        history_events_imported,
    })
}

impl NativeLibsqlProvider {
    /// Replace one instance's durable rows on `self` with rows from `src`.
    /// Returns number of history events imported.
    pub async fn import_instance_from(
        &self,
        src: &NativeLibsqlProvider,
        instance_id: &str,
        include_kv: bool,
        include_pins: bool,
    ) -> Result<u64, ProviderError> {
        let src_conn = src
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        let dst = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;

        // Prefer instances row; fall back to history-only instances (append without full start).
        let mut check = src_conn
            .query(
                "SELECT orchestration_name, orchestration_version, current_execution_id, custom_status, custom_status_version, parent_instance_id FROM instances WHERE instance_id = ?1",
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        let (orch_name, orch_ver, cur_exec, custom_status, custom_ver, parent_inst) =
            if let Some(inst) = check
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
            {
                (
                    inst.get::<String>(0).unwrap_or_else(|_| "Imported".into()),
                    inst.get::<String>(1).ok(),
                    inst.get::<i64>(2).unwrap_or(1),
                    inst.get::<String>(3).ok(),
                    inst.get::<i64>(4).unwrap_or(0),
                    inst.get::<String>(5).ok(),
                )
            } else {
                // History must exist on child.
                let mut h = src_conn
                    .query(
                        "SELECT COUNT(*) FROM history WHERE instance_id = ?1",
                        params![instance_id],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
                let n = h
                    .next()
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
                    .and_then(|r| r.get::<i64>(0).ok())
                    .unwrap_or(0);
                if n == 0 {
                    return Err(ProviderError::permanent(
                        "import_instance_from",
                        format!("instance {instance_id} not found on child (no instances/history)"),
                    ));
                }
                let mut m = src_conn
                    .query(
                        "SELECT COALESCE(MAX(execution_id), 1) FROM history WHERE instance_id = ?1",
                        params![instance_id],
                    )
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
                let max_exec = m
                    .next()
                    .await
                    .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
                    .and_then(|r| r.get::<i64>(0).ok())
                    .unwrap_or(1);
                (
                    "Imported".to_string(),
                    Some("1.0.0".into()),
                    max_exec,
                    None,
                    0,
                    None,
                )
            };
        drop(check);

        // Clear parent-side instance state (queues/locks first).
        for sql in [
            "DELETE FROM orchestrator_queue WHERE instance_id = ?1",
            "DELETE FROM worker_queue WHERE instance_id = ?1",
            "DELETE FROM instance_locks WHERE instance_id = ?1",
            "DELETE FROM history WHERE instance_id = ?1",
            "DELETE FROM executions WHERE instance_id = ?1",
            "DELETE FROM kv_store WHERE instance_id = ?1",
            "DELETE FROM kv_delta WHERE instance_id = ?1",
            "DELETE FROM process_definition_pins WHERE instance_id = ?1",
            "DELETE FROM instances WHERE instance_id = ?1",
        ] {
            let _ = dst.execute(sql, params![instance_id]).await;
        }

        // Insert instance (skip parent_instance_id FK if parent missing on dest).
        let parent_ok = if let Some(ref p) = parent_inst {
            let mut r = dst
                .query(
                    "SELECT 1 FROM instances WHERE instance_id = ?1",
                    params![p.as_str()],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
            r.next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
                .is_some()
        } else {
            false
        };
        if parent_ok {
            dst.execute(
                r#"
                INSERT INTO instances (
                  instance_id, orchestration_name, orchestration_version,
                  current_execution_id, custom_status, custom_status_version, parent_instance_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    instance_id,
                    orch_name,
                    orch_ver,
                    cur_exec,
                    custom_status,
                    custom_ver,
                    parent_inst
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        } else {
            dst.execute(
                r#"
                INSERT INTO instances (
                  instance_id, orchestration_name, orchestration_version,
                  current_execution_id, custom_status, custom_status_version, parent_instance_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)
                "#,
                params![
                    instance_id,
                    orch_name,
                    orch_ver,
                    cur_exec,
                    custom_status,
                    custom_ver
                ],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        }

        // Executions
        let mut exec_rows = src_conn
            .query(
                r#"
                SELECT execution_id, status, output,
                       duroxide_version_major, duroxide_version_minor, duroxide_version_patch
                FROM executions WHERE instance_id = ?1
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        while let Some(row) = exec_rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
        {
            let execution_id: i64 = row.get(0).unwrap_or(0);
            let status: String = row.get(1).unwrap_or_else(|_| "Running".into());
            let output: Option<String> = row.get(2).ok();
            let maj: Option<i64> = row.get(3).ok();
            let min: Option<i64> = row.get(4).ok();
            let pat: Option<i64> = row.get(5).ok();
            dst.execute(
                r#"
                INSERT INTO executions (
                  instance_id, execution_id, status, output,
                  duroxide_version_major, duroxide_version_minor, duroxide_version_patch
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![instance_id, execution_id, status, output, maj, min, pat],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        }
        drop(exec_rows);

        // History
        let mut hist = src_conn
            .query(
                r#"
                SELECT execution_id, event_id, event_type, event_data
                FROM history WHERE instance_id = ?1
                ORDER BY execution_id, event_id
                "#,
                params![instance_id],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
        let mut hist_n = 0u64;
        while let Some(row) = hist
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
        {
            let execution_id: i64 = row.get(0).unwrap_or(0);
            let event_id: i64 = row.get(1).unwrap_or(0);
            let event_type: String = row.get(2).unwrap_or_default();
            let event_data: String = row.get(3).unwrap_or_default();
            dst.execute(
                r#"
                INSERT INTO history (instance_id, execution_id, event_id, event_type, event_data)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![instance_id, execution_id, event_id, event_type, event_data],
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
            hist_n += 1;
        }
        drop(hist);

        if include_kv {
            let mut kvs = src_conn
                .query(
                    "SELECT key, value, last_updated_at_ms FROM kv_store WHERE instance_id = ?1",
                    params![instance_id],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
            while let Some(row) = kvs
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?
            {
                let key: String = row.get(0).unwrap_or_default();
                let value: String = row.get(1).unwrap_or_default();
                let ts: i64 = row.get(2).unwrap_or(0);
                dst.execute(
                    r#"
                    INSERT INTO kv_store (instance_id, key, value, last_updated_at_ms)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                    params![instance_id, key, value, ts],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("import_instance_from", e))?;
            }
        }

        if include_pins
            && let Ok(mut pins) = src_conn
                .query(
                    r#"
                    SELECT definition_name, definition_version, pinned_at_ms
                    FROM process_definition_pins WHERE instance_id = ?1
                    "#,
                    params![instance_id],
                )
                .await
            && let Ok(Some(row)) = pins.next().await
        {
            let name: String = row.get(0).unwrap_or_default();
            let ver: String = row.get(1).unwrap_or_default();
            let ts: i64 = row.get(2).unwrap_or(0);
            let _ = dst
                .execute(
                    r#"
                    INSERT INTO process_definition_pins
                      (instance_id, definition_name, definition_version, pinned_at_ms)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                    params![instance_id, name, ver, ts],
                )
                .await;
        }

        Ok(hist_n)
    }

    /// Append a promote audit row (best-effort table create).
    pub async fn record_promote_audit(
        &self,
        previous_world_id: &str,
        promoted_from_child_id: &str,
        backup_path: &str,
        note: Option<&str>,
    ) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("record_promote_audit", e))?;
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS world_promote_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                previous_world_id TEXT NOT NULL,
                promoted_from_child_id TEXT NOT NULL,
                backup_path TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                created_at_ms INTEGER NOT NULL
            )
            "#,
            (),
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("record_promote_audit", e))?;
        let now = Self::now_millis();
        conn.execute(
            r#"
            INSERT INTO world_promote_audit
              (previous_world_id, promoted_from_child_id, backup_path, note, created_at_ms)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                previous_world_id,
                promoted_from_child_id,
                backup_path,
                note.unwrap_or(""),
                now
            ],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("record_promote_audit", e))?;
        Ok(())
    }
}

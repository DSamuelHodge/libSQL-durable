//! PVM Phase 7 — World mesh.
//!
//! Many worlds with explicit peers and cross-world references. No hidden shared
//! memory: links are data rows operators and hosts can inspect.

use duroxide::providers::ProviderError;
use libsql::params;

use crate::native::NativeLibsqlProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldPeer {
    pub peer_world_id: String,
    pub endpoint: String,
    pub role: String,
    pub last_seen_ms: i64,
    pub meta_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldRef {
    pub local_instance_id: String,
    pub remote_world_id: String,
    pub remote_instance_id: String,
    pub created_at_ms: i64,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshStatus {
    pub local_world_id: Option<String>,
    pub peer_count: u64,
    pub ref_count: u64,
    pub peers: Vec<WorldPeer>,
}

impl NativeLibsqlProvider {
    pub async fn ensure_mesh_schema(&self) -> Result<(), ProviderError> {
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("ensure_mesh_schema", e))?;
        for sql in [
            r#"
            CREATE TABLE IF NOT EXISTS world_mesh_peers (
                peer_world_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                role TEXT NOT NULL,
                last_seen_ms INTEGER NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}'
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS world_refs (
                local_instance_id TEXT NOT NULL,
                remote_world_id TEXT NOT NULL,
                remote_instance_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (local_instance_id, remote_world_id, remote_instance_id)
            )
            "#,
            "CREATE INDEX IF NOT EXISTS idx_world_refs_remote ON world_refs(remote_world_id)",
        ] {
            conn.execute(sql, ())
                .await
                .map_err(|e| Self::libsql_to_provider_error("ensure_mesh_schema", e))?;
        }
        Ok(())
    }

    pub async fn register_mesh_peer(
        &self,
        peer_world_id: &str,
        endpoint: &str,
        role: &str,
        meta_json: Option<&str>,
    ) -> Result<(), ProviderError> {
        self.ensure_mesh_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("register_mesh_peer", e))?;
        let now = Self::now_millis();
        let meta = meta_json.unwrap_or("{}");
        conn.execute(
            r#"
            INSERT INTO world_mesh_peers (peer_world_id, endpoint, role, last_seen_ms, meta_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(peer_world_id) DO UPDATE SET
                endpoint = excluded.endpoint,
                role = excluded.role,
                last_seen_ms = excluded.last_seen_ms,
                meta_json = excluded.meta_json
            "#,
            params![peer_world_id, endpoint, role, now, meta],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("register_mesh_peer", e))?;
        Ok(())
    }

    pub async fn list_mesh_peers(&self) -> Result<Vec<WorldPeer>, ProviderError> {
        self.ensure_mesh_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_mesh_peers", e))?;
        let mut rows = conn
            .query(
                r#"
                SELECT peer_world_id, endpoint, role, last_seen_ms, meta_json
                FROM world_mesh_peers ORDER BY peer_world_id
                "#,
                (),
            )
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_mesh_peers", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_mesh_peers", e))?
        {
            out.push(WorldPeer {
                peer_world_id: row.get::<String>(0).unwrap_or_default(),
                endpoint: row.get::<String>(1).unwrap_or_default(),
                role: row.get::<String>(2).unwrap_or_default(),
                last_seen_ms: row.get::<i64>(3).unwrap_or(0),
                meta_json: row.get::<String>(4).unwrap_or_else(|_| "{}".into()),
            });
        }
        Ok(out)
    }

    pub async fn add_world_ref(
        &self,
        local_instance_id: &str,
        remote_world_id: &str,
        remote_instance_id: &str,
        note: Option<&str>,
    ) -> Result<(), ProviderError> {
        self.ensure_mesh_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("add_world_ref", e))?;
        let now = Self::now_millis();
        conn.execute(
            r#"
            INSERT INTO world_refs
              (local_instance_id, remote_world_id, remote_instance_id, created_at_ms, note)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(local_instance_id, remote_world_id, remote_instance_id) DO UPDATE SET
                note = excluded.note
            "#,
            params![
                local_instance_id,
                remote_world_id,
                remote_instance_id,
                now,
                note.unwrap_or("")
            ],
        )
        .await
        .map_err(|e| Self::libsql_to_provider_error("add_world_ref", e))?;
        Ok(())
    }

    pub async fn list_world_refs(
        &self,
        local_instance_id: Option<&str>,
    ) -> Result<Vec<WorldRef>, ProviderError> {
        self.ensure_mesh_schema().await?;
        let conn = self
            .connect()
            .await
            .map_err(|e| Self::libsql_to_provider_error("list_world_refs", e))?;
        let mut out = Vec::new();
        if let Some(id) = local_instance_id {
            let mut rows = conn
                .query(
                    r#"
                    SELECT local_instance_id, remote_world_id, remote_instance_id, created_at_ms, note
                    FROM world_refs WHERE local_instance_id = ?1
                    ORDER BY created_at_ms DESC
                    "#,
                    params![id],
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("list_world_refs", e))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("list_world_refs", e))?
            {
                out.push(row_to_world_ref(row));
            }
        } else {
            let mut rows = conn
                .query(
                    r#"
                    SELECT local_instance_id, remote_world_id, remote_instance_id, created_at_ms, note
                    FROM world_refs ORDER BY created_at_ms DESC LIMIT 500
                    "#,
                    (),
                )
                .await
                .map_err(|e| Self::libsql_to_provider_error("list_world_refs", e))?;
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| Self::libsql_to_provider_error("list_world_refs", e))?
            {
                out.push(row_to_world_ref(row));
            }
        }
        Ok(out)
    }

    pub async fn mesh_status(&self) -> Result<MeshStatus, ProviderError> {
        self.ensure_mesh_schema().await?;
        let peers = self.list_mesh_peers().await?;
        let refs = self.list_world_refs(None).await?;
        let local_world_id = self
            .world_manifest()
            .await
            .ok()
            .flatten()
            .map(|m| m.world_id);
        Ok(MeshStatus {
            local_world_id,
            peer_count: peers.len() as u64,
            ref_count: refs.len() as u64,
            peers,
        })
    }
}

fn row_to_world_ref(row: libsql::Row) -> WorldRef {
    WorldRef {
        local_instance_id: row.get::<String>(0).unwrap_or_default(),
        remote_world_id: row.get::<String>(1).unwrap_or_default(),
        remote_instance_id: row.get::<String>(2).unwrap_or_default(),
        created_at_ms: row.get::<i64>(3).unwrap_or(0),
        note: row.get::<String>(4).unwrap_or_default(),
    }
}

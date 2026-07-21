//! libSQL connection configuration and engine options.
//!
//! Wires the libSQL SDK capabilities that sit outside the Duroxide provider
//! contract: namespaces, encryption, background sync, offline sync, etc.

use std::path::PathBuf;
use std::time::Duration;

/// Shared libSQL engine options applied on top of a backend mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibsqlEngineOptions {
    /// Multi-tenant namespace header (`x-namespace`) for remote / replica modes.
    pub namespace: Option<String>,
    /// Local file encryption key (AES-256-CBC). Requires the `encryption` feature.
    pub local_encryption_key: Option<Vec<u8>>,
    /// Base64 remote encryption key sent as `x-turso-encryption-key`.
    pub remote_encryption_key_b64: Option<String>,
    /// Background sync interval for embedded replica / offline-synced modes.
    pub sync_interval: Option<Duration>,
    /// Whether replica/offline clients see local writes before remote ack.
    pub read_your_writes: bool,
    /// Offline-synced mode: push local writes to the remote primary.
    pub remote_writes: bool,
}

impl Default for LibsqlEngineOptions {
    fn default() -> Self {
        Self {
            namespace: None,
            local_encryption_key: None,
            remote_encryption_key_b64: None,
            sync_interval: None,
            read_your_writes: true,
            remote_writes: true,
        }
    }
}

impl LibsqlEngineOptions {
    pub fn from_env() -> Self {
        let mut options = Self::default();

        if let Ok(namespace) = std::env::var("LIBSQL_NAMESPACE") {
            let namespace = namespace.trim().to_string();
            if !namespace.is_empty() {
                options.namespace = Some(namespace);
            }
        }

        if let Ok(key) = std::env::var("LIBSQL_ENCRYPTION_KEY") {
            if !key.is_empty() {
                // Default: treat env value as raw key bytes. Set
                // LIBSQL_ENCRYPTION_KEY_BASE64=1 if the value is base64.
                if env_flag_true("LIBSQL_ENCRYPTION_KEY_BASE64") {
                    options.local_encryption_key = decode_base64_loosely(&key);
                } else {
                    options.local_encryption_key = Some(key.into_bytes());
                }
            }
        }

        if let Ok(key) = std::env::var("LIBSQL_REMOTE_ENCRYPTION_KEY") {
            if !key.is_empty() {
                options.remote_encryption_key_b64 = Some(key);
            }
        }

        if let Ok(ms) = std::env::var("LIBSQL_SYNC_INTERVAL_MS") {
            if let Ok(parsed) = ms.parse::<u64>() {
                if parsed > 0 {
                    options.sync_interval = Some(Duration::from_millis(parsed));
                }
            }
        }

        if let Ok(v) = std::env::var("LIBSQL_READ_YOUR_WRITES") {
            options.read_your_writes = env_truthy(&v);
        }
        if let Ok(v) = std::env::var("LIBSQL_REMOTE_WRITES") {
            options.remote_writes = env_truthy(&v);
        }

        options
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_local_encryption_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.local_encryption_key = Some(key.into());
        self
    }

    pub fn with_remote_encryption_key_b64(mut self, key: impl Into<String>) -> Self {
        self.remote_encryption_key_b64 = Some(key.into());
        self
    }

    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = Some(interval);
        self
    }

    pub fn with_read_your_writes(mut self, enabled: bool) -> Self {
        self.read_your_writes = enabled;
        self
    }

    pub fn with_remote_writes(mut self, enabled: bool) -> Self {
        self.remote_writes = enabled;
        self
    }
}

fn env_flag_true(name: &str) -> bool {
    std::env::var(name)
        .map(|v| env_truthy(&v))
        .unwrap_or(false)
}

fn env_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn decode_base64_loosely(input: &str) -> Option<Vec<u8>> {
    // Minimal base64 decode without extra deps: use a tiny manual alphabet map.
    // Prefer std-only for config parsing; invalid input falls back to raw bytes.
    fn decode(data: &str) -> Option<Vec<u8>> {
        const TABLE: &[u8; 128] = &{
            let mut t = [0xffu8; 128];
            let mut i = 0u8;
            while i < 26 {
                t[(b'A' + i) as usize] = i;
                t[(b'a' + i) as usize] = 26 + i;
                i += 1;
            }
            i = 0;
            while i < 10 {
                t[(b'0' + i) as usize] = 52 + i;
                i += 1;
            }
            t[b'+' as usize] = 62;
            t[b'/' as usize] = 63;
            t
        };

        let clean: Vec<u8> = data
            .bytes()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        if clean.is_empty() || clean.len() % 4 != 0 {
            return None;
        }
        let mut out = Vec::with_capacity(clean.len() / 4 * 3);
        for chunk in clean.chunks_exact(4) {
            let mut vals = [0u8; 4];
            for (i, &c) in chunk.iter().enumerate() {
                if c == b'=' {
                    vals[i] = 0;
                } else if (c as usize) < 128 {
                    let v = TABLE[c as usize];
                    if v == 0xff {
                        return None;
                    }
                    vals[i] = v;
                } else {
                    return None;
                }
            }
            out.push((vals[0] << 2) | (vals[1] >> 4));
            if chunk[2] != b'=' {
                out.push((vals[1] << 4) | (vals[2] >> 2));
            }
            if chunk[3] != b'=' {
                out.push((vals[2] << 6) | vals[3]);
            }
        }
        Some(out)
    }

    decode(input).or_else(|| Some(input.as_bytes().to_vec()))
}

/// Backend topology for the durable provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LibsqlDatabaseMode {
    InMemory,
    Local {
        path: PathBuf,
    },
    /// Pure remote HTTP/Hrana against `sqld` / Turso.
    Remote {
        url: String,
        auth_token: String,
    },
    /// Embedded remote replica (local file + sync from primary).
    RemoteReplica {
        local_path: PathBuf,
        remote_url: String,
        auth_token: String,
    },
    /// Offline-capable local DB that syncs to a remote primary.
    OfflineSynced {
        local_path: PathBuf,
        remote_url: String,
        auth_token: String,
    },
}

/// Full connection config: topology + engine options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibsqlDatabaseConfig {
    pub mode: LibsqlDatabaseMode,
    pub options: LibsqlEngineOptions,
}

impl LibsqlDatabaseConfig {
    pub fn new(mode: LibsqlDatabaseMode) -> Self {
        Self {
            mode,
            options: LibsqlEngineOptions::default(),
        }
    }

    pub fn in_memory() -> Self {
        Self::new(LibsqlDatabaseMode::InMemory)
    }

    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::new(LibsqlDatabaseMode::Local {
            path: path.into(),
        })
    }

    pub fn remote(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self::new(LibsqlDatabaseMode::Remote {
            url: url.into(),
            auth_token: auth_token.into(),
        })
    }

    pub fn remote_replica(
        local_path: impl Into<PathBuf>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Self {
        Self::new(LibsqlDatabaseMode::RemoteReplica {
            local_path: local_path.into(),
            remote_url: remote_url.into(),
            auth_token: auth_token.into(),
        })
    }

    pub fn offline_synced(
        local_path: impl Into<PathBuf>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Self {
        Self::new(LibsqlDatabaseMode::OfflineSynced {
            local_path: local_path.into(),
            remote_url: remote_url.into(),
            auth_token: auth_token.into(),
        })
    }

    pub fn with_options(mut self, options: LibsqlEngineOptions) -> Self {
        self.options = options;
        self
    }

    pub fn options_mut(&mut self) -> &mut LibsqlEngineOptions {
        &mut self.options
    }

    pub fn from_env() -> Self {
        let options = LibsqlEngineOptions::from_env();

        if let Ok(remote_url) = std::env::var("LIBSQL_REMOTE_URL") {
            let auth_token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default();
            let offline = env_flag_true("LIBSQL_OFFLINE_SYNC");

            if let Ok(local_path) = std::env::var("LIBSQL_REPLICA_PATH") {
                let mode = if offline {
                    LibsqlDatabaseMode::OfflineSynced {
                        local_path: PathBuf::from(local_path),
                        remote_url,
                        auth_token,
                    }
                } else {
                    LibsqlDatabaseMode::RemoteReplica {
                        local_path: PathBuf::from(local_path),
                        remote_url,
                        auth_token,
                    }
                };
                return Self { mode, options };
            }

            return Self {
                mode: LibsqlDatabaseMode::Remote {
                    url: remote_url,
                    auth_token,
                },
                options,
            };
        }

        let url = std::env::var("LIBSQL_DATABASE_URL")
            .unwrap_or_else(|_| "file:./stress-libsql.db".to_string());
        Self::from_local_url(&url).with_options(options)
    }

    pub fn from_local_url(url: &str) -> Self {
        if url == ":memory:" || url.contains(":memory:") || url.contains("mode=memory") {
            return Self::in_memory();
        }

        let path = url
            .strip_prefix("file:")
            .or_else(|| url.strip_prefix("sqlite:"))
            .unwrap_or(url);
        Self::local(PathBuf::from(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_url_parsing() {
        assert!(matches!(
            LibsqlDatabaseConfig::from_local_url(":memory:").mode,
            LibsqlDatabaseMode::InMemory
        ));
        match LibsqlDatabaseConfig::from_local_url("file:./x.db").mode {
            LibsqlDatabaseMode::Local { path } => {
                assert_eq!(path, PathBuf::from("./x.db"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn options_builder_chain() {
        let opts = LibsqlEngineOptions::default()
            .with_namespace("tenant-a")
            .with_sync_interval(Duration::from_secs(2))
            .with_remote_writes(false);
        assert_eq!(opts.namespace.as_deref(), Some("tenant-a"));
        assert_eq!(opts.sync_interval, Some(Duration::from_secs(2)));
        assert!(!opts.remote_writes);
    }
}

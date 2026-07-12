//! Daemon lifecycle management and HTTP API helpers.
//!
//! This module spawns a `polytoken daemon` child process, waits for it to be ready,
//! and provides thin HTTP wrapper methods for the daemon's REST API.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rand::Rng;
use serde::Deserialize;
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

/// How long to wait for the daemon to become ready (polling startup.json).
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Handle to a running polytoken daemon process.
pub struct DaemonHandle {
    base_url: String,
    bearer_token: String,
    session_id: String,
    cwd: std::path::PathBuf,
    child: Option<Child>,
    #[allow(dead_code)]
    sessions_dir: PathBuf,
    #[allow(dead_code)]
    log_dir: PathBuf,
    #[allow(dead_code)]
    cred_path: PathBuf,
}

#[derive(Deserialize)]
struct StartupJson {
    state: String,
    port: Option<u16>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Deserialize)]
struct PromptAcceptedResponse {
    prompt_id: String,
}

/// Subset of the daemon's `GET /state` response (`SessionStateSnapshot`)
/// that we need to populate the ACP model list.
#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct SessionStateSnapshot {
    #[serde(default)]
    pub active_model: Option<String>,
    #[serde(default)]
    pub available_models: Vec<AvailableModelEntry>,
}

/// One entry in the daemon's available-model list.
#[allow(dead_code)]
#[derive(Deserialize)]
pub(crate) struct AvailableModelEntry {
    pub name: String,
    pub label: String,
}

impl DaemonHandle {
    /// Spawn a new polytoken daemon for the given working directory.
    pub async fn spawn(cwd: &Path) -> Result<DaemonHandle> {
        Self::spawn_with_session_id(cwd, None).await
    }

    /// Spawn a polytoken daemon, optionally resuming an existing session by ID.
    ///
    /// When `resume_session_id` is `Some`, the daemon is started with the given
    /// session ID and will load the corresponding session history from its
    /// internal store. A fresh temp directory and credential are always
    /// created for the daemon process itself.
    pub async fn spawn_with_session_id(
        cwd: &Path,
        resume_session_id: Option<&str>,
    ) -> Result<DaemonHandle> {
        let session_id = resume_session_id
            .map(|s| s.to_string())
            .unwrap_or_else(generate_session_id);
        let temp_dir = std::env::temp_dir().join(format!("polytoken-acp-{}", session_id));
        let sessions_dir = temp_dir.join("sessions");
        let log_dir = temp_dir.join("logs");
        let cred_path = temp_dir.join("credential.json");

        // Create temp dir with 0700 permissions (polytoken requires it)
        std::fs::create_dir_all(&temp_dir).context("Failed to create temp dir")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp_dir, std::fs::Permissions::from_mode(0o700))
                .context("Failed to set temp dir permissions")?;
        }
        std::fs::create_dir_all(&sessions_dir).context("Failed to create sessions dir")?;
        std::fs::create_dir_all(&log_dir).context("Failed to create log dir")?;

        let bearer_token = generate_token();
        let cred_json = serde_json::json!({
            "version": 1,
            "kind": "polytoken-daemon-credential",
            "token": bearer_token,
        });
        std::fs::write(&cred_path, serde_json::to_string(&cred_json)?)
            .context("Failed to write credential file")?;

        // Polytoken requires the credential file to have 0600 permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&cred_path, std::fs::Permissions::from_mode(0o600))
                .context("Failed to set credential file permissions")?;
        }

        info!(session_id = %session_id, cwd = ?cwd, "Spawning polytoken daemon");

        let mut child = Command::new("polytoken")
            .arg("daemon")
            .arg("--project-dir")
            .arg(cwd)
            .arg("--credential-file")
            .arg(&cred_path)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--sessions-dir")
            .arg(&sessions_dir)
            .arg("--log-dir")
            .arg(&log_dir)
            .arg("--session-id")
            .arg(&session_id)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Failed to spawn polytoken daemon")?;

        // Drain child stdout to prevent pipe blocking; log stderr.
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 1024];
                let mut stdout = stdout;
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = vec![0u8; 4096];
                let mut stderr = stderr;
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let s = String::from_utf8_lossy(&buf[..n]);
                            for line in s.lines() {
                                debug!(target: "polytoken_daemon", "{}", line);
                            }
                        }
                    }
                }
            });
        }

        // Poll startup.json until state == "ready"
        let startup_path = sessions_dir.join(&session_id).join("startup.json");
        let timeout_result =
            tokio::time::timeout(STARTUP_TIMEOUT, poll_startup(&startup_path)).await;

        let port = match timeout_result {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => {
                let _ = child.kill().await;
                return Err(e);
            }
            Err(_) => {
                // Check if the process is still alive
                match child.try_wait() {
                    Ok(Some(status)) => bail!(
                        "Polytoken daemon exited during startup with status: {}. Check logs at {:?}",
                        status,
                        log_dir
                    ),
                    Ok(None) => {
                        let _ = child.kill().await;
                        bail!(
                            "Polytoken daemon failed to become ready within {} seconds. Check logs at {:?}",
                            STARTUP_TIMEOUT.as_secs(),
                            log_dir
                        );
                    }
                    Err(e) => bail!(
                        "Daemon startup timed out and process status check failed: {}",
                        e
                    ),
                }
            }
        };

        // Quick health check
        let base_url = format!("http://127.0.0.1:{}", port);
        let _port = port; // suppress unused warning in some paths

        let mut handle = DaemonHandle {
            base_url: base_url.clone(),
            bearer_token,
            session_id,
            cwd: cwd.to_path_buf(),
            child: Some(child),
            sessions_dir,
            log_dir,
            cred_path,
        };

        // Verify the daemon is responding
        for attempt in 0..10 {
            if handle.health().await.unwrap_or(false) {
                info!(session_id = %handle.session_id, "Daemon is healthy");
                return Ok(handle);
            }
            if attempt == 9 {
                let _ = handle.child.as_mut().unwrap().kill().await;
                bail!(
                    "Daemon startup.json says ready but /health is not responding after 10 attempts"
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        unreachable!()
    }

    /// Send a prompt to the daemon using explicit connection params (no borrow needed).
    pub async fn prompt_with(base_url: &str, bearer_token: &str, content: &str) -> Result<String> {
        let client = reqwest::Client::new();
        let url = format!("{}/prompt", base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", bearer_token))
            .json(&serde_json::json!({"content": content}))
            .send()
            .await
            .context("Failed to send prompt to daemon")?;

        let status = resp.status();
        let body: PromptAcceptedResponse = resp.json().await.context(format!(
            "Failed to parse prompt response (status {})",
            status
        ))?;

        debug!(prompt_id = %body.prompt_id, "Prompt accepted by daemon");
        Ok(body.prompt_id)
    }

    /// Send a prompt to the daemon. Returns the prompt_id.
    #[allow(dead_code)]
    pub async fn prompt(&self, content: &str) -> Result<String> {
        Self::prompt_with(&self.base_url, &self.bearer_token, content).await
    }

    /// Cancel the current turn.
    #[allow(dead_code)]
    pub async fn cancel_turn(&self) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/turn/cancel", self.base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await
            .context("Failed to cancel turn")?;
        if !resp.status().is_success() {
            bail!("Cancel turn failed with status: {}", resp.status());
        }
        Ok(())
    }

    /// Respond to an interrogative (permission request).
    #[allow(dead_code)]
    pub async fn respond_interrogative(&self, interrogative_id: &str, granted: bool) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/interrogative/{}/respond",
            self.base_url, interrogative_id
        );
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&serde_json::json!({"kind": "permission_answer", "granted": granted}))
            .send()
            .await
            .context("Failed to respond to interrogative")?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "Interrogative response failed");
        }
        Ok(())
    }

    /// Check daemon health.
    pub async fn health(&self) -> Result<bool> {
        let client = reqwest::Client::new();
        let url = format!("{}/health", self.base_url);
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await;
        match resp {
            Ok(r) => Ok(r.status().is_success()),
            Err(_) => Ok(false),
        }
    }

    /// Fetch the daemon's session state snapshot (the subset we need).
    #[allow(dead_code)]
    pub async fn fetch_session_state(&self) -> Result<SessionStateSnapshot> {
        let client = reqwest::Client::new();
        let url = format!("{}/state", self.base_url);
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await
            .context("Failed to GET /state")?;
        if !resp.status().is_success() {
            bail!("GET /state returned status {}", resp.status());
        }
        resp.json::<SessionStateSnapshot>()
            .await
            .context("Failed to parse /state response")
    }

    /// Switch the active model by POSTing to `/model`.
    #[allow(dead_code)]
    pub async fn set_model(&self, model_name: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/model", self.base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&serde_json::json!({ "model": model_name }))
            .send()
            .await
            .context("Failed to POST /model")?;
        if !resp.status().is_success() {
            bail!("POST /model returned status {}", resp.status());
        }
        Ok(())
    }

    /// The SSE events URL for this daemon.
    #[allow(dead_code)]
    pub fn events_url(&self) -> String {
        format!("{}/events", self.base_url)
    }

    /// The bearer token used for auth.
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// The base URL of the daemon.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The session ID of this daemon.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The working directory of this daemon.
    pub fn cwd(&self) -> &std::path::Path {
        &self.cwd
    }

    /// Switch the active facet by POSTing to `/facet`.
    #[allow(dead_code)]
    pub async fn set_facet(&self, facet: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/facet", self.base_url);
        let resp = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&serde_json::json!({ "facet": facet }))
            .send()
            .await
            .context("Failed to POST /facet")?;
        if !resp.status().is_success() {
            bail!("POST /facet returned status {}", resp.status());
        }
        Ok(())
    }

    /// Fetch the full daemon state snapshot.
    pub async fn fetch_daemon_state(&self) -> Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let url = format!("{}/state", self.base_url);
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await
            .context("Failed to GET /state")?;
        if !resp.status().is_success() {
            bail!("GET /state returned status {}", resp.status());
        }
        resp.json::<serde_json::Value>()
            .await
            .context("Failed to parse /state response")
    }

    /// Terminate the daemon gracefully.
    pub async fn terminate(&mut self) {
        let client = reqwest::Client::new();
        let url = format!("{}/terminate", self.base_url);
        let _ = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .send()
            .await;

        // Give it a moment then kill
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(child) = &mut self.child {
            let _ = child.kill().await;
        }
        info!(session_id = %self.session_id, "Daemon terminated");
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        // Best-effort synchronous kill if async terminate wasn't called
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

async fn poll_startup(startup_path: &Path) -> Result<u16> {
    loop {
        if let Ok(content) = std::fs::read_to_string(startup_path)
            && let Ok(startup) = serde_json::from_str::<StartupJson>(&content)
        {
            match startup.state.as_str() {
                "ready" => {
                    if let Some(port) = startup.port {
                        return Ok(port);
                    }
                    bail!("startup.json has no port field");
                }
                "failed" => {
                    let msg = startup
                        .message
                        .unwrap_or_else(|| "unknown error".to_string());
                    bail!("Polytoken daemon failed to start: {}", msg);
                }
                _ => {
                    // Still starting up
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Generate a session ID in polytoken's required format:
/// `{6 Crockford base32 chars}-{word}`
/// Crockford base32 excludes I, L, O, U to avoid confusion.
fn generate_session_id() -> String {
    const CROCKFORD: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";
    const WORDS: &[&str] = &[
        "acp", "junky", "river", "delta", "alpha", "forge", "nova", "echo", "flux", "gamma",
        "halo", "iris", "jolt", "keen", "lunar", "mega", "nimbus", "opal", "pulse", "quill",
    ];
    let mut rng = rand::thread_rng();
    let prefix: String = (0..6)
        .map(|_| {
            let idx = rng.gen_range(0..CROCKFORD.len());
            CROCKFORD[idx] as char
        })
        .collect();
    let word = WORDS[rng.gen_range(0..WORDS.len())];
    format!("{}-{}", prefix, word)
}

fn generate_token() -> String {
    let mut rng = rand::thread_rng();
    (0..48)
        .map(|_| {
            let c = rng.gen_range(0..62);
            if c < 26 {
                (b'a' + c) as char
            } else if c < 52 {
                (b'A' + c - 26) as char
            } else {
                (b'0' + c - 52) as char
            }
        })
        .collect()
}

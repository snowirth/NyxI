//! Embedding engine — semantic search via Ollama's /api/embed endpoint.
//!
//! Uses nomic-embed-text (768d) or all-minilm (384d) running in Ollama.
//! Vectors stored as blobs in SQLite. Cosine similarity computed in Rust.
//! Combined with FTS5 for hybrid search: keywords + semantics.

pub struct Embedder {
    client: reqwest::Client,
    ollama_host: String,
    model: String,
}

impl Embedder {
    pub fn new(ollama_host: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap(),
            ollama_host: ollama_host.to_string(),
            model: "qwen3-embedding:0.6b".to_string(),
        }
    }

    /// Get embedding vector for a text. Returns None if Ollama is unavailable.
    pub async fn embed(&self, text: &str) -> Option<Vec<f32>> {
        let embeddings = self.embed_many(&[text.to_string()]).await?;
        embeddings.into_iter().next()
    }

    /// Get embedding vectors for multiple texts in a single Ollama request.
    /// Returns None if Ollama is unavailable or the response shape is unexpected.
    pub async fn embed_many(&self, texts: &[String]) -> Option<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Some(Vec::new());
        }
        let body = serde_json::json!({
            "model": self.model,
            "input": texts,
        });

        let resp = self
            .client
            .post(format!("{}/api/embed", self.ollama_host))
            .json(&body)
            .send()
            .await
            .ok()?;

        if !resp.status().is_success() {
            tracing::debug!("embed: ollama returned {}", resp.status());
            return None;
        }

        let data: serde_json::Value = resp.json().await.ok()?;
        let embeddings = data["embeddings"].as_array()?;
        let parsed = embeddings
            .iter()
            .map(|embedding| {
                embedding
                    .as_array()
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect::<Vec<_>>()
                    })
                    .filter(|values| !values.is_empty())
            })
            .collect::<Option<Vec<_>>>()?;
        if parsed.len() != texts.len() {
            return None;
        }
        Some(parsed)
    }
}

/// Cosine similarity between two vectors. Returns 0.0 if either is zero-length.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot: f32 = 0.0;
    let mut norm_a: f32 = 0.0;
    let mut norm_b: f32 = 0.0;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < 1e-10 { 0.0 } else { dot / denom }
}

/// Serialize a vector to bytes for SQLite blob storage.
pub fn vec_to_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Deserialize bytes back to a vector.
pub fn bytes_to_vec(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

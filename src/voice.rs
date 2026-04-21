//! Voice — mic capture, speech-to-text, text-to-speech.
//!
//! TTS: macOS `say` command (instant, no setup)
//! STT: whisper via Python tool (transcribe.py)
//! Mic: cpal capture to WAV file, then transcribe
//!
//! Web endpoints:
//!   POST /api/voice/record — start recording
//!   POST /api/voice/stop — stop recording, transcribe, respond
//!   POST /api/voice/say — speak text aloud

use crate::AppState;

/// Speak text aloud using macOS `say` command.
pub async fn speak(text: &str) {
    let text = text.to_string();
    tokio::spawn(async move {
        // Use Samantha voice, slightly faster rate
        let _ = tokio::process::Command::new("say")
            .args(["-v", "Samantha", "-r", "190", &text])
            .output()
            .await;
    });
}

/// Speak text in the background, streaming sentence by sentence.
pub async fn speak_streaming(text: &str) {
    // Split into sentences and speak each one
    for sentence in text.split(|c: char| c == '.' || c == '!' || c == '?') {
        let sentence = sentence.trim();
        if sentence.len() > 2 {
            let s = format!("{}.", sentence);
            let _ = tokio::process::Command::new("say")
                .args(["-v", "Samantha", "-r", "190", &s])
                .output()
                .await;
        }
    }
}

/// Record audio from the microphone for a given duration.
/// Saves as WAV file and returns the path.
pub async fn record_audio(duration_secs: u32) -> Result<String, String> {
    let workspace = std::env::current_dir()
        .unwrap_or_default()
        .join("workspace")
        .join("audio");
    std::fs::create_dir_all(&workspace).ok();

    let filename = format!("rec_{}.wav", chrono::Utc::now().timestamp());
    let filepath = workspace.join(&filename);
    let path_str = filepath.to_str().unwrap_or("").to_string();

    // Use macOS `rec` (from sox) or `ffmpeg` for recording
    // Try sox first, then ffmpeg, then afrecord
    let result = tokio::process::Command::new("sox")
        .args([
            "-d",
            "-r",
            "16000",
            "-c",
            "1",
            "-b",
            "16",
            &path_str,
            "trim",
            "0",
            &duration_secs.to_string(),
        ])
        .output()
        .await;

    if let Ok(output) = result {
        if output.status.success() && filepath.exists() {
            return Ok(path_str);
        }
    }

    // Fallback: ffmpeg
    let result = tokio::process::Command::new("ffmpeg")
        .args([
            "-f",
            "avfoundation",
            "-i",
            ":0",
            "-t",
            &duration_secs.to_string(),
            "-ar",
            "16000",
            "-ac",
            "1",
            "-y",
            &path_str,
        ])
        .stderr(std::process::Stdio::null())
        .output()
        .await;

    if let Ok(output) = result {
        if output.status.success() && filepath.exists() {
            return Ok(path_str);
        }
    }

    Err("no recording tool available (install sox or ffmpeg)".into())
}

/// Transcribe audio file to text using the whisper tool.
pub async fn transcribe(audio_path: &str) -> Result<String, String> {
    match crate::tools::run("transcribe", &serde_json::json!({"audio_path": audio_path})).await {
        Ok(result) => {
            if let Some(text) = result["output"].as_str() {
                Ok(text.to_string())
            } else {
                Err(result["error"]
                    .as_str()
                    .unwrap_or("transcription failed")
                    .to_string())
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Full voice pipeline: record → transcribe → handle → speak response.
pub async fn voice_interaction(state: &AppState, duration_secs: u32) -> String {
    // Record
    tracing::info!("voice: recording {}s...", duration_secs);
    let audio_path = match record_audio(duration_secs).await {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("couldn't record: {}", e);
            speak(&msg).await;
            return msg;
        }
    };

    // Transcribe
    tracing::info!("voice: transcribing...");
    let text = match transcribe(&audio_path).await {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("couldn't transcribe: {}", e);
            speak(&msg).await;
            return msg;
        }
    };

    if text.is_empty() {
        speak("i didn't catch that").await;
        return "empty transcription".into();
    }

    tracing::info!("voice: heard \"{}\"", crate::trunc(&text, 60));

    // Handle through normal message pipeline
    let response = state.handle("voice", "user", &text).await;

    // Speak the response
    speak_streaming(&response).await;

    // Clean up audio file
    std::fs::remove_file(&audio_path).ok();

    response
}

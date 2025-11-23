use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;

#[derive(Clone, Debug)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

impl BasicAuthConfig {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn authorization_header(&self) -> String {
        let token = format!("{}:{}", self.username, self.password);
        format!("Authorization: Basic {}", BASE64.encode(token))
    }
}

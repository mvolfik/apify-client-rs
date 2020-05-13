use reqwest;
use tokio::time::delay_for;
use std::time::Duration;
use serde::{Deserialize};

// TODO: Make this configurable
const MAX_RATE_LIMIT_RETRIES: u8 = 8;
const MAX_SERVER_FAIL_RETRIES: u8 = 8;
const MAX_TIMEOUT_RETRIES: u8 = 5;

pub struct ApifyClient {
    pub token: String,
    pub client: reqwest::Client,
    pub base_path: String,
    base_time_to_retry: u32,
    debug_log: bool,
}

#[derive(Deserialize, Debug)]
pub struct ApifyApiErrorRaw {
    r#type: String,
    message: String,
}

// TODO: Remove this
#[derive(Deserialize, Debug)]
pub struct ApifyApiErrorRawWrapper {
    error: ApifyApiErrorRaw
}

#[derive(Debug)]
pub enum ApifyClientError {
    // The variant here is "type" and the param is "message"
    NotFound(String),
    // We don't have types for all statuses now so we just pass a message
    RawError(String),
    MaxTimeoutRetriesReached(u8),
    MaxRateLimitRetriesReached(u8),
    MaxServerFailedRetriesReached(u8)
}

impl ApifyClient {
    pub fn new (token: String) -> ApifyClient {
        assert_eq!(token.len(), 25);
        let client = reqwest::Client::new();
        ApifyClient {
            token,
            client,
            base_path: "https://api.apify.com/v2".to_owned(),
            base_time_to_retry: 500,
            debug_log: true,
        }
    }

    async fn simple_request (&self, url: &str, method: &reqwest::Method, body: Vec<u8>, headers: reqwest::header::HeaderMap) -> Result<reqwest::Response, reqwest::Error> {
        match method {
            &reqwest::Method::GET => self.client.get(url).send().await,
            &reqwest::Method::POST => self.client.get(url).body(body).headers(headers).send().await,
            &reqwest::Method::PUT => self.client.get(url).body(body).headers(headers).send().await,
            &reqwest::Method::DELETE => self.client.get(url).send().await,
            _ => panic!("Request method not allowed!"),
        }
    }

    pub async fn retrying_request (&self, url: &str, method: &reqwest::Method, body: Vec<u8>, headers: reqwest::header::HeaderMap) -> Result<reqwest::Response, ApifyClientError> {
        let mut rate_limit_retry_count: u8 = 0;
        let mut server_failed_retry_count: u8 = 0;
        let mut timeout_retry_count: u8 = 0;
        loop {
            if rate_limit_retry_count >= MAX_RATE_LIMIT_RETRIES {
                return Err(ApifyClientError::MaxRateLimitRetriesReached(rate_limit_retry_count));
            }
            if server_failed_retry_count >= MAX_SERVER_FAIL_RETRIES {
                return Err(ApifyClientError::MaxServerFailedRetriesReached(server_failed_retry_count));
            }
            if timeout_retry_count >= MAX_TIMEOUT_RETRIES {
                return Err(ApifyClientError::MaxTimeoutRetriesReached(timeout_retry_count));
            }
            // TODO: Remove clones (moved in the loop)
            match self.simple_request(url, method, body.clone(), headers.clone()).await {
                Ok(resp) => {
                    let status_code = resp.status().as_u16();
                    if status_code == 429 || status_code >= 500 {
                        let time_to_next_retry;
                        if status_code == 429 {
                            rate_limit_retry_count += 1;
                            // TODO: export this as separate func
                            time_to_next_retry = self.base_time_to_retry * (2 as u32).pow((rate_limit_retry_count).into());
                            if self.debug_log {
                                println!("Request got rate limit(429), retry n. will happen {} in: {} ms", rate_limit_retry_count, time_to_next_retry);
                            }
                        } else {
                            server_failed_retry_count += 1;
                            time_to_next_retry = self.base_time_to_retry * (2 as u32).pow((server_failed_retry_count).into());
                            if self.debug_log {
                                println!("Server failed({}), retry n. will happen {} in: {} ms", status_code, rate_limit_retry_count, time_to_next_retry);
                            }
                        }
                        
                        delay_for(Duration::from_millis(time_to_next_retry.into())).await;
                        continue;
                    } else if status_code >= 300 {
                        // TODO: This should never fail but still we should handle this gracefully
                        let raw_error: ApifyApiErrorRawWrapper = resp.json().await.unwrap();
                        // error route
                        if status_code == 404 {
                            return Err(ApifyClientError::NotFound(raw_error.error.message));
                        }
                        return Err(ApifyClientError::RawError(raw_error.error.message));
                        // more types here
                    } else {
                        // ok route
                        // TODO: Remove unwrap
                        return Ok(resp);
                    }
                }
                Err(err) => {
                    if err.is_timeout() {
                        timeout_retry_count += 1;
                        let time_to_next_retry = self.base_time_to_retry * (2 as u32).pow((timeout_retry_count).into());
                        if self.debug_log {
                            println!("Request timeouted, retry n. will happen {} in: {} ms", rate_limit_retry_count, time_to_next_retry);
                        }
                        delay_for(Duration::from_millis(time_to_next_retry.into())).await;
                        continue;
                    }
                    // Maybe other types here
                    panic!("ApifyClientError: Uknown error, please create an issue on GitHub! {}", err);
                }
            }
        }
    }
}
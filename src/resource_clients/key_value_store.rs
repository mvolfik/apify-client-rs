use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::apify_client::ApifyClient;
use crate::base_clients::resource_client::ResourceClient;
use crate::error::ApifyClientError;
use crate::generic_types::{BaseBuilder, NoOutput};

pub struct KeyValueStoreClient<'a> {
    pub apify_client: &'a ApifyClient,
    pub url_segment: String,
}

// See comment on the ResourceClient trait why this boilerplate is needed
impl<'a> ResourceClient<'a, KeyValueStore> for KeyValueStoreClient<'a> {
    fn get_client(&self) -> &'a ApifyClient {
        self.apify_client
    }

    fn get_url_segment(&self) -> &str {
        &self.url_segment
    }
}

impl<'kv> KeyValueStoreClient<'kv> {
    pub fn new(apify_client: &'kv ApifyClient, identifier: &str) -> Self {
        KeyValueStoreClient {
            apify_client,
            url_segment: format!("key-value-stores/{}", identifier),
        }
    }

    pub fn get_value<'key, 'builder, T: DeserializeOwned>(
        &'kv self,
        key: &'key str,
    ) -> GetValueBuilder<'builder, T>
    where
        'kv: 'builder,
        'key: 'builder,
    {
        GetValueBuilder {
            key_value_store_client: self,
            key,
            phantom: PhantomData,
        }
    }

    pub fn set_value<'props, 'builder, T>(
        &'kv self,
        key: &'props str,
        value: &'props T,
    ) -> SetValueBuilder<'builder, T>
    where
        T: Serialize,
        'kv: 'builder,
        'props: 'builder,
    {
        SetValueBuilder {
            key_value_store_client: self,
            key,
            value,
        }
    }
}

pub struct GetValueBuilder<'a, T: DeserializeOwned> {
    key_value_store_client: &'a KeyValueStoreClient<'a>,
    key: &'a str,
    phantom: PhantomData<T>,
}

impl<'a, T: DeserializeOwned> GetValueBuilder<'a, T> {
    pub async fn send(self) -> Result<T, ApifyClientError> {
        let builder: BaseBuilder<'_, T> = BaseBuilder::new(
            self.key_value_store_client.apify_client,
            format!(
                "{}/records/{}",
                self.key_value_store_client.url_segment, self.key
            ),
            Method::GET,
        );
        builder.send().await
    }
}

pub struct SetValueBuilder<'a, T: Serialize> {
    key_value_store_client: &'a KeyValueStoreClient<'a>,
    key: &'a str,
    value: &'a T,
}

impl<'a, T: Serialize> SetValueBuilder<'a, T> {
    pub async fn send(self) -> Result<NoOutput, ApifyClientError> {
        let mut builder: BaseBuilder<'_, NoOutput> = BaseBuilder::new(
            self.key_value_store_client.apify_client,
            format!(
                "{}/records/{}",
                self.key_value_store_client.url_segment, self.key
            ),
            Method::PUT,
        );
        builder.raw_payload(serde_json::to_vec(&self.value)?);
        builder.validate_and_send_request().await?;
        Ok(NoOutput)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueStore {
    pub id: String,
    pub name: Option<String>,
    pub user_id: String,
    pub created_at: String,
    pub modified_at: String,
    pub accessed_at: String,
    pub act_id: Option<String>,
    pub act_run_id: Option<String>,
}

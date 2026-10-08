//! Connects to the Zama SDK daemon and signs decryption permits.

use super::config::{PLUGIN_NAME, Settings};
use alloy_dyn_abi::TypedData;
use alloy_primitives::{Address, Signature};
use k256::ecdsa::SigningKey;
use std::collections::BTreeMap;
use streamling_plugin::PluginError;
use zama_sdk::{
    ChainConfig, Client, RelayerAuth, RelayerConfig, RelayerOptions, RelayerTransport, Sdk,
    SdkConfig, SdkError, Signer, SigningRequest, WalletAccount, async_trait,
};

/// The only EIP-712 messages the delegate key signs: the SDK's user and
/// delegated decryption permits. Anything else the daemon asks for is refused.
const PERMIT_TYPES: [&str; 2] = [
    "UserDecryptRequestVerification",
    "DelegatedUserDecryptRequestVerification",
];

/// Signs decryption permits with the delegate key. The key never leaves this
/// process; the daemon only receives signatures.
pub struct DelegateSigner {
    key: SigningKey,
    address: Address,
}

impl DelegateSigner {
    pub fn new(key: SigningKey) -> Self {
        let address = Address::from_public_key(key.verifying_key());
        Self { key, address }
    }
}

#[async_trait]
impl Signer for DelegateSigner {
    async fn sign_typed_data(&self, request: SigningRequest) -> Result<Vec<u8>, SdkError> {
        if request.account.address != self.address {
            return Err(SdkError::signing_failed(format!(
                "{PLUGIN_NAME}: signing request is for {}, not the delegate",
                request.account.address
            )));
        }
        let typed: TypedData = serde_json::from_value(request.typed_data).map_err(|_| {
            SdkError::signing_failed(format!(
                "{PLUGIN_NAME}: signing request is not EIP-712 typed data"
            ))
        })?;
        if !PERMIT_TYPES.contains(&typed.primary_type.as_str()) {
            return Err(SdkError::signing_rejected(format!(
                "{PLUGIN_NAME}: refusing to sign '{}'; the delegate key signs decryption permits only",
                typed.primary_type
            )));
        }
        let hash = typed.eip712_signing_hash().map_err(|_| {
            SdkError::signing_failed(format!(
                "{PLUGIN_NAME}: signing request cannot be hashed as EIP-712 typed data"
            ))
        })?;
        let delegator = typed.message["delegatorAddress"]
            .as_str()
            .map(|account| format!(" for delegator {account}"))
            .unwrap_or_default();
        tracing::info!(
            signer = %self.address,
            "{PLUGIN_NAME}: signing {}{delegator}",
            typed.primary_type
        );
        let (signature, recovery_id) = self
            .key
            .sign_prehash_recoverable(hash.as_slice())
            .map_err(|error| SdkError::signing_failed(format!("{PLUGIN_NAME}: {error}")))?;
        Ok(Signature::from((signature, recovery_id))
            .as_bytes()
            .to_vec())
    }
}

/// Opens a signer-enabled SDK context on the daemon.
pub async fn connect(settings: &Settings) -> Result<Sdk, PluginError> {
    let mut chain = ChainConfig::new(settings.chain_id, settings.rpc_url.clone());
    if let Some(key) = &settings.relayer_api_key {
        chain = chain.with_auth(RelayerAuth::api_key(key.clone()));
    }
    let mut config = SdkConfig::from_chains(settings.chain_id, vec![chain]);
    if settings.relayer_debug {
        config.relayers = Some(BTreeMap::from([(
            settings.chain_id,
            RelayerConfig {
                transport: RelayerTransport::Node,
                options: Some(RelayerOptions {
                    debug: Some(true),
                    ..Default::default()
                }),
            },
        )]));
    }
    let account = WalletAccount {
        address: settings.delegate_address(),
        chain_id: settings.chain_id,
    };
    let client = Client::connect(&settings.daemon_socket)
        .await
        .map_err(|error| {
            internal(format!(
                "connecting to daemon at {}: {error}",
                settings.daemon_socket.display()
            ))
        })?;
    let mut builder = client
        .sdk(config)
        .signer(Some(account), DelegateSigner::new(settings.signer.clone()))
        .storage(settings.storage.clone());
    if let Some(secret) = &settings.derivation_secret {
        builder = builder.transport_key_pair_derivation_secret(secret.clone());
    }
    builder
        .build()
        .await
        .map_err(|error| internal(format!("creating SDK context: {error}")))
}

fn internal(message: String) -> PluginError {
    PluginError::Internal(format!("{PLUGIN_NAME}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use serde_json::json;

    const KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    fn signer() -> DelegateSigner {
        let key = KEY.parse::<B256>().expect("hex");
        DelegateSigner::new(SigningKey::from_slice(key.as_slice()).expect("key"))
    }

    fn request(primary_type: &str, signer: &DelegateSigner) -> SigningRequest {
        SigningRequest {
            operation_id: "op".into(),
            action_id: "act".into(),
            account: WalletAccount {
                address: signer.address,
                chain_id: 1,
            },
            typed_data: json!({
                "types": {
                    "EIP712Domain": [{ "name": "name", "type": "string" }],
                    primary_type: [{ "name": "delegatorAddress", "type": "address" }]
                },
                "primaryType": primary_type,
                "domain": { "name": "Decryption" },
                "message": { "delegatorAddress": "0x066931a63774a135a5a449d042bc77cb697958d1" }
            }),
        }
    }

    fn typed_hash(request: &SigningRequest) -> B256 {
        serde_json::from_value::<TypedData>(request.typed_data.clone())
            .expect("typed data")
            .eip712_signing_hash()
            .expect("hash")
    }

    #[test]
    fn address_is_derived_from_the_key() {
        assert_eq!(
            signer().address,
            "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
                .parse::<Address>()
                .expect("address")
        );
    }

    #[tokio::test]
    async fn signs_decryption_permits_only() {
        let signer = signer();
        let permit = request("DelegatedUserDecryptRequestVerification", &signer);
        let signature = signer
            .sign_typed_data(permit.clone())
            .await
            .expect("permit is signed");
        assert_eq!(signature.len(), 65);
        // The signature recovers to the delegate address over the EIP-712 hash.
        let recovered = Signature::from_raw(&signature)
            .expect("signature")
            .recover_address_from_prehash(&typed_hash(&permit))
            .expect("recover");
        assert_eq!(recovered, signer.address);

        let refused = signer
            .sign_typed_data(request("Permit", &signer))
            .await
            .expect_err("other typed data is refused");
        assert!(refused.message.contains("refusing to sign 'Permit'"));

        let mut other = request("UserDecryptRequestVerification", &signer);
        other.account.address = Address::ZERO;
        assert!(signer.sign_typed_data(other).await.is_err());
    }
}

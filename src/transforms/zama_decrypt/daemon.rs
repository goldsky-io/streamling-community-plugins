//! Connects to the Zama SDK daemon and signs decryption permits.

use super::config::{PLUGIN_NAME, Settings};
use alloy_dyn_abi::TypedData;
use alloy_primitives::{Address, Signature, U256};
use k256::ecdsa::SigningKey;
use streamling_plugin::PluginError;
use zama_sdk::{
    ChainConfig, Client, RelayerAuth, Sdk, SdkConfig, SdkError, Signer, SigningRequest, Storage,
    WalletAccount, async_trait,
};

/// The delegate key signs the SDK's two decryption permits and nothing else
/// the daemon may ask for.
const PERMIT_TYPES: [&str; 2] = [
    "UserDecryptRequestVerification",
    "DelegatedUserDecryptRequestVerification",
];

/// The key never leaves this process; the daemon only receives signatures.
pub struct DelegateSigner {
    key: SigningKey,
    address: Address,
    chain_id: U256,
}

impl DelegateSigner {
    pub fn new(key: SigningKey, chain_id: u64) -> Self {
        let address = Address::from_public_key(key.verifying_key());
        Self {
            key,
            address,
            chain_id: U256::from(chain_id),
        }
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
        if typed.domain.chain_id != Some(self.chain_id) {
            return Err(SdkError::signing_rejected(format!(
                "{PLUGIN_NAME}: refusing to sign '{}' for domain chain {:?}; the delegate key signs for chain {} only",
                typed.primary_type, typed.domain.chain_id, self.chain_id
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

/// Memory storage means every restart generates a new transport key pair and
/// re-signs one permit per delegating wallet on first use.
pub async fn connect(settings: &Settings) -> Result<Sdk, PluginError> {
    let mut chain = ChainConfig::new(settings.chain_id, settings.rpc_url.clone());
    if let Some(key) = &settings.relayer_api_key {
        chain = chain.with_auth(RelayerAuth::api_key(key.clone()));
    }
    let config = SdkConfig::from_chains(settings.chain_id, vec![chain]);
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
    client
        .sdk(config)
        .signer(
            Some(account),
            DelegateSigner::new(settings.signer.clone(), settings.chain_id),
        )
        .storage(Storage::Memory)
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
    const CHAIN_ID: u64 = 11_155_111;

    fn signer() -> DelegateSigner {
        let key = KEY.parse::<B256>().expect("hex");
        DelegateSigner::new(
            SigningKey::from_slice(key.as_slice()).expect("key"),
            CHAIN_ID,
        )
    }

    fn request(primary_type: &str, chain_id: u64, signer: &DelegateSigner) -> SigningRequest {
        SigningRequest {
            operation_id: "op".into(),
            action_id: "act".into(),
            account: WalletAccount {
                address: signer.address,
                chain_id,
            },
            typed_data: json!({
                "types": {
                    "EIP712Domain": [
                        { "name": "name", "type": "string" },
                        { "name": "chainId", "type": "uint256" }
                    ],
                    primary_type: [{ "name": "delegatorAddress", "type": "address" }]
                },
                "primaryType": primary_type,
                "domain": { "name": "Decryption", "chainId": chain_id },
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
        let permit = request("DelegatedUserDecryptRequestVerification", CHAIN_ID, &signer);
        let signature = signer
            .sign_typed_data(permit.clone())
            .await
            .expect("permit is signed");
        assert_eq!(signature.len(), 65);
        let recovered = Signature::from_raw(&signature)
            .expect("signature")
            .recover_address_from_prehash(&typed_hash(&permit))
            .expect("recover");
        assert_eq!(recovered, signer.address);

        let refused = signer
            .sign_typed_data(request("Permit", CHAIN_ID, &signer))
            .await
            .expect_err("other typed data is refused");
        assert!(refused.message.contains("refusing to sign 'Permit'"));

        let mut other = request("UserDecryptRequestVerification", CHAIN_ID, &signer);
        other.account.address = Address::ZERO;
        assert!(signer.sign_typed_data(other).await.is_err());
    }

    #[tokio::test]
    async fn refuses_permits_for_another_chain() {
        let signer = signer();
        let refused = signer
            .sign_typed_data(request("UserDecryptRequestVerification", 1, &signer))
            .await
            .expect_err("wrong chain is refused");
        assert_eq!(refused.code, "SIGNING_REJECTED");
        assert!(
            refused.message.contains("domain chain"),
            "{}",
            refused.message
        );

        let mut missing = request("UserDecryptRequestVerification", CHAIN_ID, &signer);
        missing.typed_data["domain"] = json!({ "name": "Decryption" });
        missing.typed_data["types"]["EIP712Domain"] = json!([{ "name": "name", "type": "string" }]);
        assert!(signer.sign_typed_data(missing).await.is_err());
    }
}

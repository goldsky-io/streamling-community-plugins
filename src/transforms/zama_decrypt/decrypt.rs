//! Turns decoded transfers into cleartext amounts. Each handle is decrypted on
//! behalf of one of the two transfer parties that delegated to us. Rows with
//! no delegating party, or that the relayer rejects, are reported in place;
//! a daemon that cannot be reached fails the whole batch instead.

use super::decode::DecodedTransfer;
use alloy_primitives::{Address, B256};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use zama_sdk::{
    BatchItem, ClearValue, ClientError, DelegatedBatchOptions, DelegationQuery, DelegationStatus,
    EncryptedInput, ErrorKind, Sdk, SdkError, async_trait,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decrypted {
    /// Decimal string so 64-bit unsigned amounts survive every sink.
    pub amount: Option<String>,
    pub delegator: Option<Address>,
    pub error: Option<String>,
}

/// A failed daemon call, split by who has to deal with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendError {
    /// The SDK answered: the request itself was refused (ACL, relayer, input).
    /// Reported on the affected rows; the batch still goes through.
    Sdk(String),
    /// The daemon could not be reached, or the context on it is gone. The
    /// batch fails so the host retries it, and the connection is rebuilt.
    Connection(String),
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sdk(message) | Self::Connection(message) => f.write_str(message),
        }
    }
}

impl From<ClientError> for BackendError {
    fn from(error: ClientError) -> Self {
        match error.kind() {
            ErrorKind::Sdk | ErrorKind::InvalidInput => Self::Sdk(error.to_string()),
            kind => Self::Connection(format!("{error} ({kind:?})")),
        }
    }
}

/// The daemon calls the decryptor needs. A trait so the selection, caching
/// and error handling can be tested without a daemon.
#[async_trait]
pub trait Backend: Send + Sync {
    async fn delegation_status(
        &self,
        query: DelegationQuery,
    ) -> Result<DelegationStatus, BackendError>;

    async fn batch_decrypt(
        &self,
        inputs: &[EncryptedInput],
        delegator: Address,
        max_concurrency: Option<u32>,
    ) -> Result<Vec<BatchItem>, BackendError>;

    async fn close(&self) -> Result<(), BackendError>;
}

pub struct SdkBackend(pub Sdk);

#[async_trait]
impl Backend for SdkBackend {
    async fn delegation_status(
        &self,
        query: DelegationQuery,
    ) -> Result<DelegationStatus, BackendError> {
        Ok(self.0.delegations().get_status(query).await?)
    }

    async fn batch_decrypt(
        &self,
        inputs: &[EncryptedInput],
        delegator: Address,
        max_concurrency: Option<u32>,
    ) -> Result<Vec<BatchItem>, BackendError> {
        let options = DelegatedBatchOptions {
            max_concurrency,
            ..Default::default()
        };
        Ok(self
            .0
            .decryption()
            .delegated_batch_decrypt_values(inputs, delegator, options)
            .await?)
    }

    async fn close(&self) -> Result<(), BackendError> {
        Ok(self.0.close().await?)
    }
}

pub struct Decryptor<B> {
    backend: B,
    delegate: Address,
    max_concurrency: Option<u32>,
    delegations: Mutex<HashMap<(Address, Address), Delegation>>,
}

#[derive(Clone, Copy)]
struct Delegation {
    active: bool,
    checked_at: Instant,
    expiry_timestamp: u64,
}

/// How long an active answer is trusted. Bounded because a permanent
/// delegation has no expiry to fall back on, and revocations must be seen.
const ACTIVE_RECHECK: Duration = Duration::from_secs(5 * 60);
/// How long an inactive answer is trusted before asking the chain again.
const INACTIVE_RECHECK: Duration = Duration::from_secs(60);

impl<B: Backend> Decryptor<B> {
    pub fn new(backend: B, delegate: Address, max_concurrency: Option<u32>) -> Self {
        Self {
            backend,
            delegate,
            max_concurrency,
            delegations: Mutex::new(HashMap::new()),
        }
    }

    pub async fn close(&self) -> Result<(), BackendError> {
        self.backend.close().await
    }

    /// Output has the same length as `rows`; entries without a transfer stay
    /// default. `Err` means the daemon is unusable and nothing was produced.
    pub async fn decrypt(
        &self,
        rows: &[Option<DecodedTransfer>],
    ) -> Result<Vec<Decrypted>, BackendError> {
        let mut output = vec![Decrypted::default(); rows.len()];
        // Group by delegator: the batch RPC decrypts for one delegator per call.
        let mut groups: HashMap<Address, Vec<(usize, DecodedTransfer)>> = HashMap::new();
        for (row, transfer) in rows.iter().enumerate() {
            let Some(transfer) = transfer else { continue };
            match self.pick_delegator(transfer).await {
                Ok(Some(delegator)) => groups.entry(delegator).or_default().push((row, *transfer)),
                Ok(None) => {
                    output[row].error = Some("no active delegation from either party".into());
                }
                Err(BackendError::Sdk(message)) => {
                    output[row].error = Some(format!("delegation check failed: {message}"));
                }
                Err(error) => return Err(error),
            }
        }
        for (delegator, entries) in groups {
            for (row, _) in &entries {
                output[*row].delegator = Some(delegator);
            }
            // The same handle may appear more than once (replayed logs); ask once.
            let mut seen: HashSet<(B256, Address)> = HashSet::new();
            let inputs: Vec<EncryptedInput> = entries
                .iter()
                .filter(|(_, transfer)| seen.insert((transfer.handle, transfer.token)))
                .map(|(_, transfer)| EncryptedInput {
                    encrypted_value: transfer.handle,
                    contract_address: transfer.token,
                })
                .collect();
            let items = match self
                .backend
                .batch_decrypt(&inputs, delegator, self.max_concurrency)
                .await
            {
                Ok(items) => items,
                Err(BackendError::Sdk(message)) => {
                    tracing::warn!(%delegator, "zama_decrypt: batch decrypt failed: {message}");
                    for (row, transfer) in entries {
                        self.forget(transfer.token, delegator).await;
                        output[row].error = Some(format!("batch decrypt failed: {message}"));
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            let failures = items.iter().filter(|item| item.result.is_err()).count();
            tracing::info!(
                %delegator,
                handles = items.len(),
                failures,
                "zama_decrypt: batch decrypted"
            );
            let by_handle: HashMap<B256, Result<ClearValue, SdkError>> = items
                .into_iter()
                .map(|item| (item.encrypted_value, item.result))
                .collect();
            for (row, transfer) in entries {
                let cell = &mut output[row];
                match by_handle.get(&transfer.handle) {
                    Some(Ok(value)) => match render(value) {
                        Ok(amount) => cell.amount = Some(amount),
                        Err(error) => cell.error = Some(error),
                    },
                    Some(Err(error)) => {
                        // The delegation may have been revoked; re-read it next time.
                        self.forget(transfer.token, delegator).await;
                        cell.error = Some(format!("{}: {}", error.code, error.message));
                    }
                    None => cell.error = Some("relayer returned no result for handle".into()),
                }
            }
        }
        Ok(output)
    }

    /// Prefers the receiver, since balances are what downstream consumers track.
    async fn pick_delegator(
        &self,
        transfer: &DecodedTransfer,
    ) -> Result<Option<Address>, BackendError> {
        for party in [transfer.to, transfer.from] {
            if party == Address::ZERO {
                continue;
            }
            if self.is_delegated(transfer.token, party).await? {
                return Ok(Some(party));
            }
        }
        Ok(None)
    }

    async fn is_delegated(&self, token: Address, delegator: Address) -> Result<bool, BackendError> {
        let now = Instant::now();
        if let Some(cached) = self
            .delegations
            .lock()
            .await
            .get(&(token, delegator))
            .copied()
        {
            let age = now.saturating_duration_since(cached.checked_at);
            let fresh = if cached.active {
                age < ACTIVE_RECHECK && !expired(cached.expiry_timestamp)
            } else {
                age < INACTIVE_RECHECK
            };
            if fresh {
                return Ok(cached.active);
            }
        }
        let status = self
            .backend
            .delegation_status(DelegationQuery {
                contract_address: token,
                delegator_address: delegator,
                delegate_address: self.delegate,
            })
            .await?;
        tracing::debug!(
            %token,
            %delegator,
            active = status.is_active,
            expiry = status.expiry_timestamp,
            "zama_decrypt: delegation status fetched"
        );
        self.delegations.lock().await.insert(
            (token, delegator),
            Delegation {
                active: status.is_active,
                checked_at: now,
                expiry_timestamp: status.expiry_timestamp,
            },
        );
        Ok(status.is_active)
    }

    async fn forget(&self, token: Address, delegator: Address) {
        self.delegations.lock().await.remove(&(token, delegator));
    }
}

/// A clock that cannot be read counts as expired, so the chain is asked again.
fn expired(expiry_timestamp: u64) -> bool {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|now| now.as_secs() >= expiry_timestamp)
        .unwrap_or(true)
}

fn render(value: &ClearValue) -> Result<String, String> {
    match value {
        ClearValue::BigInt(value) => Ok(value.to_string()),
        ClearValue::Number(value) => Ok(value.to_string()),
        ClearValue::Bool(value) => Ok(u8::from(*value).to_string()),
        ClearValue::String(value) => Ok(value.clone()),
        ClearValue::Undefined => Err("relayer returned an undefined value".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use zama_sdk::{BigInt, PERMANENT_DELEGATION_EXPIRY};

    const TOKEN: Address = Address::repeat_byte(0x11);
    const ALICE: Address = Address::repeat_byte(0xaa);
    const BOB: Address = Address::repeat_byte(0xbb);
    const DELEGATE: Address = Address::repeat_byte(0xdd);
    const HANDLE: B256 = B256::repeat_byte(0x01);
    const OTHER_HANDLE: B256 = B256::repeat_byte(0x02);

    fn transfer(from: Address, to: Address, handle: B256) -> Option<DecodedTransfer> {
        Some(DecodedTransfer {
            token: TOKEN,
            from,
            to,
            handle,
        })
    }

    fn sdk_error(code: &str) -> SdkError {
        SdkError {
            code: code.into(),
            message: "nope".into(),
            retryable: false,
            retry_after_seconds: None,
            revert_data: None,
        }
    }

    /// Scripted daemon: delegation answers per delegator, one batch answer
    /// per call in order, and a record of what was asked.
    type BatchAnswer = Result<Vec<(B256, Result<ClearValue, SdkError>)>, BackendError>;

    #[derive(Default)]
    struct Fake {
        delegations: HashMap<Address, Result<DelegationStatus, BackendError>>,
        batches: StdMutex<Vec<BatchAnswer>>,
        status_calls: StdMutex<Vec<Address>>,
        batch_calls: StdMutex<Vec<(Address, Vec<B256>)>>,
    }

    impl Fake {
        fn delegated(mut self, delegator: Address, expiry_timestamp: u64) -> Self {
            self.delegations.insert(
                delegator,
                Ok(DelegationStatus {
                    is_active: true,
                    expiry_timestamp,
                }),
            );
            self
        }

        fn batch(self, answer: Vec<(B256, Result<ClearValue, SdkError>)>) -> Self {
            self.batches.lock().unwrap().push(Ok(answer));
            self
        }

        fn batch_error(self, error: BackendError) -> Self {
            self.batches.lock().unwrap().push(Err(error));
            self
        }

        fn status_calls(&self) -> usize {
            self.status_calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl Backend for Fake {
        async fn delegation_status(
            &self,
            query: DelegationQuery,
        ) -> Result<DelegationStatus, BackendError> {
            assert_eq!(query.delegate_address, DELEGATE);
            self.status_calls
                .lock()
                .unwrap()
                .push(query.delegator_address);
            self.delegations
                .get(&query.delegator_address)
                .cloned()
                .unwrap_or(Ok(DelegationStatus {
                    is_active: false,
                    expiry_timestamp: 0,
                }))
        }

        async fn batch_decrypt(
            &self,
            inputs: &[EncryptedInput],
            delegator: Address,
            _max_concurrency: Option<u32>,
        ) -> Result<Vec<BatchItem>, BackendError> {
            self.batch_calls.lock().unwrap().push((
                delegator,
                inputs.iter().map(|input| input.encrypted_value).collect(),
            ));
            let mut batches = self.batches.lock().unwrap();
            assert!(!batches.is_empty(), "unexpected batch call");
            batches.remove(0).map(|items| {
                items
                    .into_iter()
                    .map(|(handle, result)| BatchItem {
                        encrypted_value: handle,
                        contract_address: TOKEN,
                        result,
                    })
                    .collect()
            })
        }

        async fn close(&self) -> Result<(), BackendError> {
            Ok(())
        }
    }

    fn with(fake: Fake) -> Decryptor<Fake> {
        Decryptor::new(fake, DELEGATE, None)
    }

    #[tokio::test]
    async fn receiver_is_preferred_and_sender_is_the_fallback() {
        let fake = Fake::default()
            .delegated(ALICE, PERMANENT_DELEGATION_EXPIRY)
            .batch(vec![
                (HANDLE, Ok(ClearValue::BigInt(BigInt::from(7)))),
                (OTHER_HANDLE, Ok(ClearValue::Number(8))),
            ]);
        let decryptor = with(fake);
        let rows = [
            transfer(BOB, ALICE, HANDLE),       // receiver delegated
            transfer(ALICE, BOB, OTHER_HANDLE), // only the sender delegated
            transfer(BOB, Address::ZERO, HANDLE),
            None,
        ];
        let out = decryptor.decrypt(&rows).await.expect("batch ok");
        assert_eq!(out[0].amount.as_deref(), Some("7"));
        assert_eq!(out[0].delegator, Some(ALICE));
        assert_eq!(out[1].amount.as_deref(), Some("8"));
        assert_eq!(out[1].delegator, Some(ALICE));
        assert_eq!(
            out[2].error.as_deref(),
            Some("no active delegation from either party")
        );
        assert_eq!(out[3], Decrypted::default());
        // Alice and Bob were each asked once; later lookups hit the cache, and
        // both of Alice's rows went out in one batch call.
        assert_eq!(decryptor.backend.status_calls(), 2);
        assert_eq!(decryptor.backend.batch_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn duplicate_handles_are_asked_once_and_filled_twice() {
        let fake = Fake::default()
            .delegated(ALICE, PERMANENT_DELEGATION_EXPIRY)
            .batch(vec![(HANDLE, Ok(ClearValue::Number(5)))]);
        let decryptor = with(fake);
        let rows = [transfer(BOB, ALICE, HANDLE), transfer(BOB, ALICE, HANDLE)];
        let out = decryptor.decrypt(&rows).await.expect("batch ok");
        assert_eq!(out[0].amount.as_deref(), Some("5"));
        assert_eq!(out[1].amount.as_deref(), Some("5"));
        let calls = decryptor.backend.batch_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, vec![HANDLE]);
    }

    #[tokio::test]
    async fn relayer_rejection_is_a_row_error_and_evicts_the_delegation() {
        let fake = Fake::default()
            .delegated(ALICE, PERMANENT_DELEGATION_EXPIRY)
            .batch(vec![(HANDLE, Err(sdk_error("NOT_ENTITLED")))])
            .batch(vec![(HANDLE, Ok(ClearValue::Number(1)))]);
        let decryptor = with(fake);
        let rows = [transfer(BOB, ALICE, HANDLE)];
        let out = decryptor.decrypt(&rows).await.expect("batch ok");
        assert_eq!(out[0].error.as_deref(), Some("NOT_ENTITLED: nope"));
        assert_eq!(out[0].delegator, Some(ALICE));
        assert_eq!(decryptor.backend.status_calls(), 1);
        // The cache entry is gone, so the next batch re-reads the ACL.
        decryptor.decrypt(&rows).await.expect("batch ok");
        assert_eq!(decryptor.backend.status_calls(), 2);
    }

    #[tokio::test]
    async fn sdk_level_batch_failure_marks_rows_without_failing_the_batch() {
        let fake = Fake::default()
            .delegated(ALICE, PERMANENT_DELEGATION_EXPIRY)
            .batch_error(BackendError::Sdk("relayer said no".into()));
        let decryptor = with(fake);
        let out = decryptor
            .decrypt(&[transfer(BOB, ALICE, HANDLE)])
            .await
            .expect("batch ok");
        assert_eq!(
            out[0].error.as_deref(),
            Some("batch decrypt failed: relayer said no")
        );
    }

    #[tokio::test]
    async fn connection_failure_fails_the_batch() {
        let fake = Fake::default()
            .delegated(ALICE, PERMANENT_DELEGATION_EXPIRY)
            .batch_error(BackendError::Connection("context lost".into()));
        let decryptor = with(fake);
        let error = decryptor
            .decrypt(&[transfer(BOB, ALICE, HANDLE)])
            .await
            .expect_err("must fail");
        assert_eq!(error, BackendError::Connection("context lost".into()));

        let mut fake = Fake::default();
        fake.delegations
            .insert(ALICE, Err(BackendError::Connection("socket closed".into())));
        let decryptor = with(fake);
        assert!(
            decryptor
                .decrypt(&[transfer(BOB, ALICE, HANDLE)])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn expired_delegation_is_re_read_every_time() {
        let fake = Fake::default()
            .delegated(ALICE, 1) // expired long ago, but the chain still says active
            .batch(vec![(HANDLE, Ok(ClearValue::Number(1)))])
            .batch(vec![(HANDLE, Ok(ClearValue::Number(1)))]);
        let decryptor = with(fake);
        let rows = [transfer(BOB, ALICE, HANDLE)];
        decryptor.decrypt(&rows).await.expect("batch ok");
        decryptor.decrypt(&rows).await.expect("batch ok");
        assert_eq!(decryptor.backend.status_calls(), 2);
    }

    #[test]
    fn amounts_render_as_decimal_strings() {
        assert_eq!(
            render(&ClearValue::BigInt(BigInt::from(u64::MAX))),
            Ok(u64::MAX.to_string())
        );
        assert_eq!(render(&ClearValue::Number(7)), Ok("7".to_string()));
        assert_eq!(render(&ClearValue::Bool(true)), Ok("1".to_string()));
        assert!(render(&ClearValue::Undefined).is_err());
    }
}

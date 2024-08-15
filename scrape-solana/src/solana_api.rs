use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::UiConfirmedBlock;
use std::{
    fmt::Debug,
    sync::Mutex,
    time::{Duration, Instant},
};

const MIN_WAIT: Duration = Duration::from_millis(10000 / 100); // 100 reqs/10s per IP
const BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: None,
    rewards: None,
    commitment: None,
    max_supported_transaction_version: Some(0),
};

struct SolanaApiInner {
    wait: Duration,
    last_access: Instant,
    client: RpcClient,
}

pub struct SolanaApi(Mutex<SolanaApiInner>);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Solana client error")]
    SolanaClient(#[source] solana_client::client_error::ClientError),

    #[error("Request timed out")]
    Timeout(#[source] solana_client::client_error::ClientError),

    #[error("A previous request timed out and the cooldown period has not yet expired.")]
    PostTimeoutCooldown,
}

const MAX_TIMEOUT: Duration = Duration::from_secs(30);

impl SolanaApi {
    pub fn new(endpoint_url: String) -> Self {
        let client = RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());

        SolanaApi(Mutex::new(SolanaApiInner {
            wait: MIN_WAIT,
            last_access: Instant::now() - MIN_WAIT,
            client,
        }))
    }

    pub fn fetch_accounts(
        &self,
        account_ids: &[solana_sdk::pubkey::Pubkey],
    ) -> Result<Vec<Option<solana_sdk::account::Account>>, Error> {
        self.0
            .lock()
            .unwrap()
            .do_req(|c| c.get_multiple_accounts(account_ids))
    }

    pub fn get_block_height(&self) -> Result<u64, Error> {
        self.0.lock().unwrap().do_req(|c| c.get_block_height())
    }

    pub fn fetch_block(&self, slot: u64) -> Result<Option<UiConfirmedBlock>, Error> {
        let mut inner = self.0.lock().unwrap();
        match inner.do_req(|c| c.get_block_with_config(slot, BLOCK_CONFIG).map(Some)) {
            Ok(b) => Ok(b),
            Err(Error::SolanaClient(ClientError {
                kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32009, .. }),
                ..
            })) => {
                eprintln!(
                    "skipped block slot={slot}: not present in Solana nodes nor long-term storage"
                );
                Ok(None)
            }
            Err(Error::SolanaClient(ClientError {
                kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32007, .. }),
                ..
            })) => {
                eprintln!("skipped block slot={slot}: skipped, or missing due to ledger jump to recent snapshot");
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl SolanaApiInner {
    fn do_req<R: Debug>(
        &mut self,
        mut f: impl FnMut(&mut RpcClient) -> solana_client::client_error::Result<R>,
    ) -> Result<R, Error> {
        let elapsed = self.last_access.elapsed();
        let to_sleep = self.wait.saturating_sub(elapsed);
        if to_sleep > MAX_TIMEOUT {
            // break the sleep into chunks to avoid stalling tasks for too long
            std::thread::sleep(MAX_TIMEOUT);
            return Err(Error::PostTimeoutCooldown);
        } else if !Duration::is_zero(&to_sleep) {
            std::thread::sleep(to_sleep);
        }

        let res = match f(&mut self.client) {
            Ok(r) => {
                if self.wait != MIN_WAIT {
                    self.wait -= Duration::from_millis(1);
                    self.wait = self.wait.max(MIN_WAIT);
                }
                Ok(r)
            }
            Err(ClientError {
                kind: ClientErrorKind::Reqwest(inner_e),
                request,
            }) if inner_e.is_timeout() => {
                self.wait *= 2;
                Err(Error::Timeout(ClientError {
                    kind: ClientErrorKind::Reqwest(inner_e),
                    request,
                }))
            }
            Err(
                e @ ClientError {
                    kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }),
                    ..
                },
            ) if code == -32004 || code == -32014 || code == -32016 => {
                self.wait *= 2;
                Err(Error::Timeout(e))
            }
            Err(
                e @ ClientError {
                    kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32602, .. }),
                    ..
                },
            ) => {
                self.wait *= 2;

                // this is a really bad one, we need to wait a big while
                std::thread::sleep(self.wait.max(Duration::from_secs(30)));
                Err(Error::Timeout(e))
            }
            Err(
                e @ ClientError {
                    kind: ClientErrorKind::Io(_),
                    ..
                },
            ) => {
                self.wait *= 2;
                Err(Error::Timeout(e))
            }
            Err(e) => Err(Error::SolanaClient(e)),
        };

        self.last_access = Instant::now();

        res
    }
}

use eyre::{eyre, WrapErr};
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::UiConfirmedBlock;
use std::{
    collections::BTreeSet,
    sync::Mutex,
    time::{Duration, Instant},
};

use crate::model::{Account, AccountID, Block};

const MIN_WAIT: Duration = Duration::from_millis(10000 / 100); // 100 reqs/10s per IP
const BLOCK_CONFIG: RpcBlockConfig = RpcBlockConfig {
    encoding: None,
    transaction_details: None,
    rewards: None,
    commitment: None,
    max_supported_transaction_version: Some(0),
};

struct SolanaApiInner {
    last_access: Instant,
    client: RpcClient,
}

pub struct SolanaApi(Mutex<SolanaApiInner>);

const MAX_TIMEOUT: Duration = Duration::from_secs(128);

impl SolanaApi {
    pub fn new(endpoint_url: String) -> Self {
        let client = RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());

        SolanaApi(Mutex::new(SolanaApiInner {
            last_access: Instant::now() - MIN_WAIT,
            client,
        }))
    }

    pub fn fetch_accounts(&self, account_ids: BTreeSet<AccountID>) -> eyre::Result<Vec<Account>> {
        let account_ids = account_ids
            .into_iter()
            .map(|id| solana_sdk::pubkey::Pubkey::new_from_array(id.to_owned().into()))
            .collect::<Vec<_>>();

        let (min_height, max_height, accounts) = self.fetch_accounts_inner(&account_ids)?;

        let accounts = account_ids
            .into_iter()
            .zip(accounts)
            .inspect(|(id, maybe_account)| {
                if maybe_account.is_none() {
                    eprintln!("missing account: {id}");
                }
            })
            .filter_map(|(id, maybe_account)| maybe_account.map(|account| (id, account)))
            .map(|(id, account)| Account {
                id: id.into(),
                owner: account.owner.into(),
                data: account.data,
                is_executable: account.executable,
                min_height,
                max_height,
            })
            .collect();

        Ok(accounts)
    }

    fn fetch_accounts_inner(
        &self,
        account_ids: &[solana_sdk::pubkey::Pubkey],
    ) -> eyre::Result<(u64, u64, Vec<Option<solana_sdk::account::Account>>)> {
        let mut inner = self.0.lock().unwrap();
        let now = Instant::now();
        let elapsed = now - inner.last_access;
        if elapsed < MIN_WAIT {
            std::thread::sleep(MIN_WAIT - elapsed);
        }

        let min_height = inner.client.get_block_height()?;
        std::thread::sleep(MIN_WAIT);
        let accounts = inner.client.get_multiple_accounts(account_ids)?;
        std::thread::sleep(MIN_WAIT);
        inner.last_access = Instant::now();
        let max_height = inner.client.get_block_height()?;

        Ok((min_height, max_height, accounts))
    }

    pub fn fetch_block(&self, slot: u64) -> eyre::Result<Option<UiConfirmedBlock>> {
        let mut inner = self.0.lock().unwrap();

        let now = Instant::now();
        let elapsed = now - inner.last_access;
        if elapsed < MIN_WAIT {
            std::thread::sleep(MIN_WAIT - elapsed);
        }
        inner.last_access = Instant::now();

        let mut larger_timeout = MIN_WAIT * 2;
        loop {
            match inner
                .client
                .get_block_with_config(slot, BLOCK_CONFIG)
                .map(Some)
            {
                Ok(b) => break Ok(b),
                Err(ClientError {
                    kind: ClientErrorKind::Reqwest(e),
                    ..
                }) if e.is_timeout() => {
                    eprintln!("block slot=#{slot} fetch timeout, retrying in {larger_timeout:#?}");
                    if larger_timeout > MAX_TIMEOUT {
                        break Err(eyre!("block slot=#{slot} fetch timeout, aborting fetch"));
                    }
                    std::thread::sleep(larger_timeout);
                    larger_timeout *= 2;
                    continue;
                }
                Err(ClientError {
                    kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }),
                    ..
                }) if code == -32004 || code == -32014 || code == -32016 => {
                    eprintln!("block slot=#{slot} fetch timeout, retrying in {larger_timeout:#?}");
                    if larger_timeout > MAX_TIMEOUT {
                        break Err(eyre!("block slot=#{slot} fetch timeout, aborting fetch"));
                    }
                    std::thread::sleep(larger_timeout);
                    larger_timeout *= 2;
                    continue;
                }
                Err(ClientError {
                    kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32009, .. }),
                    ..
                }) => {
                    eprintln!("skipped block slot={slot}: not present in Solana nodes nor long-term storage");
                    return Ok(None);
                }
                Err(ClientError {
                    kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32007, .. }),
                    ..
                }) => {
                    eprintln!("skipped block slot={slot}: skipped, or missing due to ledger jump to recent snapshot");
                    return Ok(None);
                }
                r @ Err(_) => {
                    let r = r.wrap_err(format!("failed to fetch next block slot={slot}"));
                    break r;
                }
            }
        }
    }
}

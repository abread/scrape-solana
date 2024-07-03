use eyre::{eyre, Context, Result};
use scrape_solana::{AccountID, Db};
use std::{
    fmt::Debug,
    io,
    ops::Range,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use clap::Parser;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(clap::Parser)]
#[command(name = "scape-solana", version, about, long_about = None)]
struct App {
    /// Database file path
    #[arg(short = 'r', long, default_value = "solana_data_db")]
    db_root_path: PathBuf,

    /// Solana API Endpoint (can be mainnet-beta, devnet, testnet or the URL for another endpoint)
    #[arg(short = 'u', long, default_value = "devnet")]
    endpoint_url: String,

    /// Sharded fetching (format: N:id where id is between 0 and N-1). Node id fetches blocks where blocknum % N = id
    #[arg(short, long, default_value = "1:0")]
    shard_config: ShardConfig,
}

#[derive(Clone, Debug)]
struct ShardConfig {
    n: u64,
    id: u64,
}

fn main() -> Result<()> {
    let args = App::parse();
    let db = unsafe { Db::open(args.db_root_path, io::stdout()) }.wrap_err("failed to open db")?;

    let endpoint_url = match args.endpoint_url.as_ref() {
        "devnet" => "https://api.devnet.solana.com",
        "testnet" => "https://api.testnet.solana.com",
        "mainnet-beta" => "https://api.mainnet-beta.solana.com",
        x => x,
    };

    let client = RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());
    let next_blocknum = if let Some(bnum) = db.last_block_num() {
        bnum.saturating_sub(args.shard_config.n)
    } else {
        println!("empty dataset: fetching latest blocknum");
        let b = client
            .get_block_height()
            .wrap_err("failed to get latest blocknum")?;
        println!("latest block is {b}");

        let b_shard = b % args.shard_config.n;
        if b_shard != args.shard_config.id {
            b.saturating_sub(b_shard + args.shard_config.n - args.shard_config.id)
        } else {
            b
        }
    };

    assert!(
        next_blocknum % args.shard_config.n == args.shard_config.id,
        "mismatch between last stored block and shard configuration. expected shard id {}, got {}",
        args.shard_config.id,
        next_blocknum % args.shard_config.n
    );

    let db = Arc::new(Mutex::new(db));

    let db2 = Arc::clone(&db); // capture db
    ctrlc::set_handler(move || {
        println!("saving db...");
        db2.lock().unwrap().sync().unwrap();
        println!("db saved, exiting");
        std::process::exit(0);
    })
    .wrap_err("could not set Ctrl+C handler")?;

    let db2 = Arc::clone(&db); // capture db
    std::thread::spawn(move || {
        db2.lock().unwrap().sync().unwrap();
        std::thread::sleep(Duration::from_secs(5))
    });

    std::thread::sleep(Duration::from_millis(500)); // ensure we respect rate limits

    let mut block_config = RpcBlockConfig::default();
    block_config.max_supported_transaction_version = Some(0);
    let block_config = block_config;

    const MIN_WAIT: Duration = Duration::from_millis(10000 / 100); // 100 reqs/10s per IP

    let account_fetcher = |ids: &[AccountID]| -> eyre::Result<(
        Vec<Option<solana_sdk::account::Account>>,
        Range<u64>,
    )> {
        println!("fetching accounts {:?}", &ids);
        let ids = ids
            .iter()
            .map(|id| solana_sdk::pubkey::Pubkey::new_from_array(id.to_owned().into()))
            .collect::<Vec<_>>();
        let min_height = client.get_block_height()?;
        std::thread::sleep(MIN_WAIT);
        let accounts = client.get_multiple_accounts(&ids)?;
        std::thread::sleep(MIN_WAIT);
        let max_height = client.get_block_height()?;
        std::thread::sleep(MIN_WAIT);

        Ok((accounts, min_height..max_height))
    };

    for block_num in (0..=next_blocknum)
        .rev()
        .step_by(args.shard_config.n as usize)
    {
        // retry after reqwest retries
        let mut larger_timeout = Duration::from_secs(1);
        let block = loop {
            match client.get_block_with_config(block_num, block_config) {
                Ok(b) => break Ok(b),
                Err(ClientError {
                    kind: ClientErrorKind::Reqwest(e),
                    ..
                }) if e.is_timeout() => {
                    eprintln!("block #{block_num} fetch timeout, retrying in {larger_timeout:#?}");
                    std::thread::sleep(larger_timeout);
                    larger_timeout *= 2;
                    continue;
                }
                Err(e) => break Err(e),
            }
        };

        // handle fetch errors
        let block = match block {
            Ok(b) => b,
            Err(ClientError {
                kind: ClientErrorKind::RpcError(RpcError::RpcResponseError { code: -32009, .. }),
                ..
            }) => {
                eprintln!(
                    "skipped block {block_num}: not present in Solana nodes nor long-term storage"
                );
                continue;
            }
            r @ Err(_) => {
                let r = r.wrap_err(format!("failed to fetch next block {block_num}"));
                eprintln!("{:?}", r.unwrap_err());
                continue;
            }
        };

        let save_start = Instant::now();
        {
            let mut d = db.lock().unwrap();
            d.store_block(block, account_fetcher)
                .wrap_err_with(|| format!("failed to store block {block_num}"))?;
        }
        let save_dur = Instant::now().duration_since(save_start);

        println!("fetched and saved block {block_num}");
        std::thread::sleep(MIN_WAIT - save_dur.min(MIN_WAIT));
    }

    Ok(())
}

impl FromStr for ShardConfig {
    type Err = eyre::Error;
    fn from_str(s: &str) -> eyre::Result<Self> {
        let (n, id) = s
            .split_once(':')
            .ok_or_else(|| eyre!("shard config missing delimiter ':'"))?;

        let n = n
            .parse::<u64>()
            .wrap_err("invalid shard config: field n must be a positive integer")?;
        let id = id.parse::<u64>().wrap_err(
            "invalid shard config: field id must be a non-negative integer smaller than n",
        )?;

        if n == 0 {
            Err(eyre!(
                "invalid shard config: field n must be a positive integer"
            ))
        } else if id >= n {
            Err(eyre!("invalid shard config: field id must be 0 <= id < n"))
        } else {
            Ok(ShardConfig { n, id })
        }
    }
}

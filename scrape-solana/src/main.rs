use eyre::{eyre, Result, WrapErr};
use scrape_solana::{solana_api::SolanaApi, workers};
use std::{fmt::Debug, path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(clap::Parser, Clone)]
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

    /// Chance of trying to fetch a block ahead of the middle slot.
    #[arg(short, long, default_value = "0.5")]
    forward_fetch_chance: f64,
}

#[derive(Clone, Debug)]
struct ShardConfig {
    n: u64,
    id: u64,
}

fn main() -> Result<()> {
    let args = App::parse();

    let endpoint_url = match args.endpoint_url.as_ref() {
        "devnet" => "https://api.devnet.solana.com",
        "testnet" => "https://api.testnet.solana.com",
        "mainnet-beta" => "https://api.mainnet-beta.solana.com",
        x => x,
    }
    .to_owned();

    let default_middle_slot_getter = {
        let args = args.clone();
        let endpoint_url = endpoint_url.to_owned();
        move || {
            let client =
                RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());
            let slot = client
                .get_epoch_info()
                .expect("failed to get latest block slot")
                .absolute_slot;

            let slot_shard = slot % args.shard_config.n;
            let slot = if slot_shard != args.shard_config.id {
                slot.saturating_sub(slot_shard + args.shard_config.n - args.shard_config.id)
            } else {
                slot
            };

            assert!(
                slot % args.shard_config.n == args.shard_config.id,
                "mismatch between last stored block and shard configuration. expected shard id {}, got {}",
                args.shard_config.id,
                slot % args.shard_config.n
            );

            slot
        }
    };

    let api = Arc::new(SolanaApi::new(endpoint_url.to_owned()));
    let (db_tx, db_handle) =
        workers::spawn_db_worker(args.db_root_path, default_middle_slot_getter);
    let (block_handler_tx, block_handler_handle) =
        workers::spawn_block_handler(Arc::clone(&api), db_tx.clone());
    let (block_fetcher_tx, block_fetcher_handle) = workers::spawn_block_fetcher(
        args.forward_fetch_chance,
        args.shard_config.n,
        api,
        block_handler_tx,
        db_tx,
    );

    ctrlc::set_handler(move || {
        println!("received stop signal");
        block_fetcher_tx
            .send(workers::BlockFetcherOperation::Stop)
            .expect("could not send stop signal to block fetcher");
    })
    .wrap_err("could not set Ctrl+C handler")?;

    block_handler_handle
        .join()
        .expect("block handler panicked")?;
    db_handle.join().expect("db worker panicked")?;
    block_fetcher_handle
        .join()
        .expect("block fetcher panicked")?;

    println!("done");
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

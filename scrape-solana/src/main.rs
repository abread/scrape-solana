use eyre::{eyre, Result, WrapErr};
use scrape_solana::{actors, db, solana_api::SolanaApi};
use std::{fmt::Debug, path::PathBuf, str::FromStr, sync::Arc};

use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(clap::Parser)]
#[command(name = "scape-solana", version, about, long_about = None)]
struct App {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    scrape_args: ScrapeArgs,
}

#[derive(clap::Subcommand)]
enum Command {
    Scrape(ScrapeArgs),
}

#[derive(clap::Args)]
struct ScrapeArgs {
    /// Database path
    #[arg(short = 'r', long, default_value = "solana_data_db")]
    db_root_path: PathBuf,

    /// Solana API Endpoint (can be mainnet-beta, devnet, testnet or the URL for another endpoint)
    #[arg(short = 'u', long, default_value = "devnet")]
    endpoint_url: SolanaEndpoint,

    /// Sharded fetching (format: N:id where id is between 0 and N-1). Node id fetches blocks where blocknum % N = id
    #[arg(short, long, default_value = "1:0")]
    shard_config: ShardConfig,

    /// Chance of trying to fetch a block ahead of the middle slot.
    #[arg(short, long, default_value = "0.01")]
    forward_fetch_chance: f64,
}

#[derive(Clone, Debug)]
struct SolanaEndpoint(String);

#[derive(Clone, Copy, Debug)]
struct ShardConfig {
    n: u64,
    id: u64,
}

fn main() -> Result<()> {
    let args = App::parse();
    let command = args.command.unwrap_or(Command::Scrape(args.scrape_args));

    match command {
        Command::Scrape(scrape_args) => scrape(scrape_args),
    }
}

fn scrape(args: ScrapeArgs) -> Result<()> {
    let default_middle_slot_getter_builder = {
        let endpoint_url = args.endpoint_url.to_owned();
        move || {
            let endpoint_url = endpoint_url.to_owned();
            move || {
                println!("fetching latest block slot");
                let client =
                    RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());
                let slot = client
                    .get_epoch_info()
                    .expect("failed to get latest block slot")
                    .absolute_slot;

                let slot = slot - 100; // avoid fetching the latest block, start a bit behind

                let slot_shard = slot % args.shard_config.n;
                let slot = if slot_shard != args.shard_config.id {
                    slot.saturating_sub(slot_shard + args.shard_config.n - args.shard_config.id)
                } else {
                    slot
                };

                assert!(
                    slot % args.shard_config.n == args.shard_config.id,
                    "faulty shard adjustment. expected shard id {}, got {}",
                    args.shard_config.id,
                    slot % args.shard_config.n
                );

                slot
            }
        }
    };

    {
        let db = db::open(
            args.db_root_path.clone(),
            default_middle_slot_getter_builder(),
            std::io::stdout(),
        )?;

        for block in db.left_blocks().take(10).chain(db.right_blocks().take(10)) {
            let slot = block?.slot;
            assert!(
                slot % args.shard_config.n == args.shard_config.id,
                "mismatch between stored block and shard configuration. expected shard id {}, got {}",
                args.shard_config.id,
                slot % args.shard_config.n
            );
        }
    }

    let api = Arc::new(SolanaApi::new(args.endpoint_url));
    let (db_tx, db_handle) =
        actors::spawn_db_actor(args.db_root_path, default_middle_slot_getter_builder());
    let (block_handler_tx, block_handler_handle) =
        actors::spawn_block_handler(Arc::clone(&api), db_tx.clone());
    let (block_fetcher_tx, block_fetcher_handle) = actors::spawn_block_fetcher(
        args.forward_fetch_chance,
        args.shard_config.n,
        api,
        block_handler_tx,
        db_tx,
    );

    ctrlc::set_handler(move || {
        println!("received stop signal");
        block_fetcher_tx
            .send(actors::BlockFetcherOperation::Stop)
            .expect("could not send stop signal to block fetcher");
    })
    .wrap_err("could not set Ctrl+C handler")?;

    let bh_res = block_handler_handle.join();
    let db_res = db_handle.join();
    let bf_res = block_fetcher_handle.join();

    bh_res.expect("block handler panicked")?;
    bf_res.expect("block fetcher panicked")?;
    db_res.expect("db actor panicked")?;

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

impl FromStr for SolanaEndpoint {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SolanaEndpoint(
            match s {
                "devnet" => "https://api.devnet.solana.com",
                "testnet" => "https://api.testnet.solana.com",
                "mainnet-beta" => "https://api.mainnet-beta.solana.com",
                x => x,
            }
            .to_owned(),
        ))
    }
}
impl std::fmt::Display for SolanaEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

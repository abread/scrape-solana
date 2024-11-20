use eyre::{eyre, Result, WrapErr};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use scrape_solana::{
    actors,
    db::{self, DbStats},
    solana_api::SolanaApi,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

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
    /// Scrape Solana blockchain
    Scrape(ScrapeArgs),
    /// Display statistic on one or more Solana databases
    Stats(StatsArgs),
    /// Display fast-computed stats on one or more Solana databases
    QuickStats(StatsArgs),
    /// Fully heal a corrupted database fetching any missing blocks/txs.
    FullHeal(ScrapeArgs),
    /// Compute a checksum summarizing all data in one or more Solana DBs
    Checksum(StatsArgs),
    /// Find blocks without timestamp
    FindBlocksWithoutTs(StatsArgs),
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

    /// UNUSED: Chance of trying to fetch a block ahead of the middle slot.
    #[arg(short, long, default_value = "0.01")]
    forward_fetch_chance: f64,
}

#[derive(clap::Args)]
struct StatsArgs {
    /// Database paths
    #[arg(default_value = "solana_data_db", num_args(1..))]
    db_paths: Vec<PathBuf>,
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
        Command::Stats(stats_args) => stats(stats_args),
        Command::QuickStats(stats_args) => quickstats(stats_args),
        Command::FullHeal(args) => full_heal(args),
        Command::Checksum(args) => checksum(args),
        Command::FindBlocksWithoutTs(args) => find_blocks_no_ts(args),
    }
}

fn scrape(args: ScrapeArgs) -> Result<()> {
    let default_middle_slot_getter_builder = || {
        let endpoint_url = args.endpoint_url.to_owned();
        let shard_config = args.shard_config;
        move || {
            println!("fetching latest block slot");
            let client =
                RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());
            let slot = client
                .get_epoch_info()
                .expect("failed to get latest block slot")
                .absolute_slot;

            let slot = slot - 100; // avoid fetching the latest block, start a bit behind

            let slot_shard = slot % shard_config.n;
            let slot = if slot_shard != shard_config.id {
                slot.saturating_sub(slot_shard + shard_config.n - shard_config.id)
            } else {
                slot
            };

            assert!(
                slot % shard_config.n == shard_config.id,
                "faulty shard adjustment. expected shard id {}, got {}",
                shard_config.id,
                slot % shard_config.n
            );

            slot
        }
    };

    {
        let db = db::open_or_create(
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

    let api = Arc::new(SolanaApi::new(args.endpoint_url.clone()));
    let (db_tx, db_handle) =
        actors::spawn_db_actor(args.db_root_path, default_middle_slot_getter_builder());
    let (block_handler_tx, _, block_handler_handle, block_converter_handle) =
        actors::spawn_block_handler(Arc::clone(&api), db_tx.clone());
    let (block_fetcher_tx, block_fetcher_handle) =
        actors::spawn_block_fetcher(args.shard_config.n, api, block_handler_tx, db_tx);

    ctrlc::set_handler(move || {
        println!("received stop signal");
        block_fetcher_tx
            .send(actors::BlockFetcherOperation::Stop)
            .expect("could not send stop signal to block fetcher");
    })
    .wrap_err("could not set Ctrl+C handler")?;

    let bc_res = block_converter_handle.join();
    let bh_res = block_handler_handle.join();
    let db_res = db_handle.join();
    let bf_res = block_fetcher_handle.join();

    bc_res.expect("block converter panicked")?;
    bh_res.expect("block handler panicked")?;
    bf_res.expect("block fetcher panicked")?;
    db_res.expect("db actor panicked")?;

    println!("done");
    Ok(())
}

fn quickstats(args: StatsArgs) -> eyre::Result<()> {
    struct QuickDbStats {
        n_blocks: u64,
        n_txs: u64,
        ts_start: Option<chrono::DateTime<chrono::Utc>>,
        ts_end: Option<chrono::DateTime<chrono::Utc>>,
    }

    #[derive(Default, Debug)]
    struct GlobalQuickDbStats {
        n_blocks: u64,
        n_txs: u64,
        ts_start_min: Option<chrono::DateTime<chrono::Utc>>,
        ts_start_max: Option<chrono::DateTime<chrono::Utc>>,
        ts_end_min: Option<chrono::DateTime<chrono::Utc>>,
        ts_end_max: Option<chrono::DateTime<chrono::Utc>>,
    }

    // remove duplicates
    let db_paths: HashSet<_> = args.db_paths.into_iter().collect();

    // compute stats for all databases
    let db_stats = db_paths
        .into_par_iter()
        .map(|p| {
            db::open(p.clone(), std::io::stdout())
                .map(|db| QuickDbStats {
                    n_blocks: db.block_count(),
                    n_txs: db.tx_count(),
                    ts_start: db
                        .left_blocks()
                        .filter_map(|mb| mb.ok())
                        .filter_map(|b| b.ts)
                        .next()
                        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0)),
                    ts_end: db
                        .right_blocks()
                        .rev()
                        .filter_map(|mb| mb.ok())
                        .filter_map(|b| b.ts)
                        .next()
                        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0)),
                })
                .map_err(|e| (p, e))
        })
        .filter_map(|maybe_stats| match maybe_stats {
            Ok(stats) => Some(GlobalQuickDbStats {
                n_blocks: stats.n_blocks,
                n_txs: stats.n_txs,
                ts_start_min: stats.ts_start,
                ts_start_max: stats.ts_start,
                ts_end_min: stats.ts_end,
                ts_end_max: stats.ts_end,
            }),
            Err((p, e)) => {
                println!("error opening db {p:?}: {e:#?}");
                None
            }
        })
        .reduce(GlobalQuickDbStats::default, |a, b| GlobalQuickDbStats {
            n_blocks: a.n_blocks + b.n_blocks,
            n_txs: a.n_txs + b.n_txs,
            ts_start_min: a
                .ts_start_min
                .map(|ts| ts.min(b.ts_start_min.unwrap_or(ts)))
                .or(b.ts_start_min),
            ts_start_max: a
                .ts_start_max
                .map(|ts| ts.max(b.ts_start_max.unwrap_or(ts)))
                .or(b.ts_start_max),
            ts_end_min: a
                .ts_end_min
                .map(|ts| ts.min(b.ts_end_min.unwrap_or(ts)))
                .or(b.ts_end_min),
            ts_end_max: a
                .ts_end_max
                .map(|ts| ts.max(b.ts_end_max.unwrap_or(ts)))
                .or(b.ts_end_max),
        });

    println!("{:#?}", db_stats);

    Ok(())
}

fn stats(args: StatsArgs) -> eyre::Result<()> {
    // remove duplicates
    let db_paths: HashSet<_> = args.db_paths.into_iter().collect();

    // compute stats for all databases
    let db_stats: Vec<_> = db_paths
        .into_par_iter()
        .map(|p| {
            let maybe_db = db::open(p.clone(), std::io::stdout());
            (p, maybe_db.map(|mut db| db.stats()))
        })
        .collect();

    #[derive(Debug, Default)]
    struct GlobalStats {
        pub total_blocks: u64,
        pub total_txs: u64,
        pub total_corrupted_block_recs: u64,
        pub total_corrupted_block_rec_chunks: u64,
        pub total_corrupted_txs: u64,
        pub total_corrupted_tx_chunks: u64,
        pub total_missing_blocks: u64,
        pub inner_ts_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
        pub outer_ts_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    }
    impl GlobalStats {
        fn merge(&mut self, db_stats: &DbStats) {
            let corrupted_block_recs =
                db_stats.left_corrupted_block_recs + db_stats.right_corrupted_block_recs;
            let corrupted_block_txs = db_stats.left_corrupted_txs + db_stats.right_corrupted_txs;

            self.total_blocks += db_stats.left_blocks_count + db_stats.right_blocks_count;
            self.total_txs += db_stats.left_txs_count + db_stats.right_txs_count;
            self.total_corrupted_block_recs += corrupted_block_recs;
            self.total_corrupted_block_rec_chunks +=
                corrupted_block_recs.div_ceil(db_stats.bcs as u64);
            self.total_corrupted_txs += corrupted_block_txs;
            self.total_corrupted_tx_chunks += corrupted_block_txs.div_ceil(db_stats.txcs as u64);
            self.total_missing_blocks +=
                db_stats.left_missing_blocks + db_stats.right_missing_blocks;
            if let Some((start, end)) = db_stats.ts_range {
                self.inner_ts_range = match self.inner_ts_range {
                    Some((start2, end2)) => Some((start.max(start2), end.min(end2))),
                    None => Some((start, end)),
                };
                self.outer_ts_range = match self.outer_ts_range {
                    Some((start2, end2)) => Some((start.min(start2), end.max(end2))),
                    None => Some((start, end)),
                };
            }
        }
    }

    // print stats while computing global stats
    println!("================================================================================");
    let mut global_stats = GlobalStats::default();
    for (path, stats) in db_stats {
        if let Ok(stats) = &stats {
            global_stats.merge(stats);
        }

        println!("database {path:?}: {stats:#?}");
        println!(
            "--------------------------------------------------------------------------------"
        );
    }
    println!("================================================================================");
    println!("Global DB stats: {global_stats:#?}");

    Ok(())
}

fn full_heal(args: ScrapeArgs) -> eyre::Result<()> {
    let old_db = db::open(args.db_root_path.clone(), std::io::stdout())?;
    let middle_slot = old_db.slot_limits()?.middle_slot;

    println!("creating new db");
    let new_path = args.db_root_path.join("full-heal");
    {
        let mut new_db =
            db::open_or_create(new_path.to_owned(), || middle_slot, std::io::stdout())?;
        new_db.sync()?;

        eprintln!(
            "discarding corrupted data from new db from previous full-heal attempts (if any)"
        );
        let (discarded_blocks, discarded_accounts) = new_db.discard_after_corrupted()?;
        eprintln!(" -> discarded {discarded_blocks} blocks and {discarded_accounts} accounts");

        new_db.sync()?;
    }

    println!("copying data from old db where possible, filling in the gaps from the network");

    let api = Arc::new(SolanaApi::new(args.endpoint_url.clone()));
    let (db_tx, db_handle) = actors::spawn_db_actor(new_path.to_owned(), move || middle_slot);
    let (_, block_handler_tx, block_handler_handle, block_converter_handle) =
        actors::spawn_block_handler(Arc::clone(&api), db_tx.clone());
    let (healer_tx, healer_handle) = actors::spawn_db_full_healer(
        args.db_root_path.clone(),
        args.shard_config.n,
        api,
        block_handler_tx,
        db_tx,
    );

    ctrlc::set_handler(move || {
        println!("received stop signal");
        healer_tx
            .send(actors::DBFullHealerOperation::Cancel)
            .expect("could not send stop signal to healer");
    })
    .wrap_err("could not set Ctrl+C handler")?;

    let bc_res = block_converter_handle.join();
    let bh_res = block_handler_handle.join();
    let db_res = db_handle.join();
    let h_res = healer_handle.join();

    bc_res.expect("block converter panicked")?;
    bh_res.expect("block handler panicked")?;
    h_res.expect("healer panicked")?;
    db_res.expect("db actor panicked")?;

    println!("done copying data");

    println!("renaming old db to temp path and new db to final path");
    let old_path = args.db_root_path.as_path().with_extension({
        let mut ext = args
            .db_root_path
            .extension()
            .map(|r| r.to_owned())
            .unwrap_or_default();
        ext.push(".old");
        ext
    });
    std::fs::rename(&args.db_root_path, &old_path)
        .wrap_err("failed to rename old DB to temporary path")?;
    std::fs::rename(old_path.join("full-heal"), &args.db_root_path)
        .wrap_err("failed to rename new DB to final path")?;

    println!("db moved to final path, removing old db");
    std::fs::remove_dir_all(old_path).wrap_err("failed to remove old DB")?;

    println!("heal complete!");

    Ok(())
}

fn checksum(args: StatsArgs) -> eyre::Result<()> {
    // ensure there are no duplicates
    let db_paths: HashSet<_> = args.db_paths.into_iter().collect();

    let checksums = db_paths
        .into_par_iter()
        .map(|db_path| {
            (
                db_path.clone(),
                db::open(db_path.clone(), std::io::stdout())
                    .wrap_err_with(|| eyre!("failed to open db {db_path:?}"))
                    .map(|mut db| db.checksum()),
            )
        })
        .collect::<HashMap<_, eyre::Result<_>>>();

    for (path, maybe_csum) in checksums {
        println!("checksum for {path:?}:\t{maybe_csum:#X?}");
    }

    Ok(())
}

fn find_blocks_no_ts(args: StatsArgs) -> eyre::Result<()> {
    // ensure there are no duplicates
    let db_paths: HashSet<_> = args.db_paths.into_iter().collect();

    let blocks_no_ts = db_paths
        .into_par_iter()
        .map(|db_path| {
            (
                db_path.clone(),
                db::open(db_path.clone(), std::io::stdout())
                    .wrap_err_with(|| eyre!("failed to open db {db_path:?}"))
                    .map(|db| {
                        db.blocks()
                            .filter_map(|mb| {
                                let b = mb.expect("corrupted block");
                                if b.ts.is_none() {
                                    Some(b.slot)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                    }),
            )
        })
        .collect::<HashMap<_, eyre::Result<_>>>();

    for (path, maybe_blocks) in blocks_no_ts {
        println!("blocks (slots) without timestamp in {path:?}:\t{maybe_blocks:#?}");
    }

    Ok(())
}

impl FromStr for ShardConfig {
    type Err = eyre::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

use eyre::{Context, Result};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use clap::Parser;
use mmap_vec::MmapVec;
use solana_client::{
    client_error::{reqwest::Url, ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};
use solana_sdk::commitment_config::CommitmentConfig;

const EXPECTED_BLOCK_COUNT: usize = 128;
const EXPECTED_TXS_PER_BLOCK: usize = 1645; // experimentally obatined from a 10 block sample

#[derive(clap::Parser)]
#[command(name = "scape-solana", version, about, long_about = None)]
struct App {
    /// Database file path
    #[arg(short, long, default_value = "solana_data_db")]
    db_root_path: PathBuf,

    /// Solana API Endpoint (can be mainnet-beta, devnet, testnet or the URL for another endpoint)
    #[arg(short, long, default_value = "devnet")]
    endpoint_url: String,
}

#[repr(C, packed)]
#[derive(Default, Debug)]
struct TxRecord {
    block_id: u64,
    fee: u64,
    compute_units: u64,
}

#[repr(C, packed)]
#[derive(Default, Debug)]
struct BlockRecord {
    block_id: u64,
    ts: i64,
    tx_count: u64,
}

#[derive(Debug)]
struct Db {
    blocks: MmapVec<BlockRecord>,
    txs: MmapVec<TxRecord>,
}

fn main() -> Result<()> {
    let args = App::parse();
    let mut db = open_db(args.db_root_path).wrap_err("failed to open db")?;

    let endpoint_url = match args.endpoint_url.as_ref() {
        "devnet" => "https://api.devnet.solana.com",
        "testnet" => "https://api.testnet.solana.com",
        "mainnet-beta" => "https://api.mainnet-beta.solana.com",
        x => x,
    };

    let client = RpcClient::new_with_commitment(endpoint_url, CommitmentConfig::finalized());
    let next_block = if let Some(b) = db.blocks.last() {
        b.block_id.saturating_sub(1)
    } else {
        println!("empty dataset: fetching latest blocknum");
        client
            .get_block_height()
            .wrap_err("failed to get latest blocknum")?
    };

    // cleanup semi-fetched txs
    let mut trunc_idx = db.txs.len() - 1;
    for i in (0..db.txs.len()).rev() {
        if db.txs[i].block_id == next_block {
            trunc_idx = i;
        } else {
            break;
        }
    }
    if (trunc_idx + 1) != db.txs.len() {
        println!(
            "dropping {} txs from block {next_block} (possible partial fetch)",
            db.txs.len() - trunc_idx - 1
        );
        db.txs.truncate(trunc_idx + 1);
    }

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
    for block_num in (0..next_block).rev() {
        let mut larger_timeout = Duration::from_millis(500);
        let block = loop {
            match client.get_block_with_config(block_num, block_config) {
                Ok(b) => break Ok(b),
                Err(ClientError {
                    kind: ClientErrorKind::Reqwest(e),
                    ..
                }) if e.is_timeout() => {
                    eprintln!("block #{block_num} fetch timeout, retrying");
                    std::thread::sleep(larger_timeout);
                    larger_timeout *= 2;
                    continue;
                }
                Err(e) => break Err(e),
            }
        };

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
            Err(e) => return Err(e).wrap_err("failed to fetch next block"),
        };

        let now = Instant::now();

        {
            let mut d = db.lock().unwrap();
            let txs = block.transactions.unwrap_or(Vec::new());

            let block_rec = BlockRecord {
                block_id: block_num,
                ts: block.block_time.unwrap_or(i64::MAX),
                tx_count: txs.len() as u64,
            };

            for tx in txs {
                let tx_rec = TxRecord {
                    block_id: block_num,
                    fee: tx.meta.as_ref().map(|m| m.fee).unwrap_or(u64::MAX),
                    compute_units: tx
                        .meta
                        .as_ref()
                        .map(|m| m.compute_units_consumed.clone().unwrap_or(u64::MAX))
                        .unwrap_or(u64::MAX),
                };

                d.txs.push(tx_rec).wrap_err("could not save tx")?;
            }
            d.blocks.push(block_rec).wrap_err("could not save block")?;
        }

        let now2 = Instant::now();
        println!("fetched and saved block {block_num}");
        std::thread::sleep(
            Duration::from_millis(500) - (now2 - now).min(Duration::from_millis(500)),
        );
    }

    Ok(())
}

impl Db {
    fn sync(&mut self) -> Result<()> {
        self.txs.sync().wrap_err("could not sync txs table")?;
        self.blocks.sync().wrap_err("could not sync blocks table")?;
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // The MmapVec destructor deletes the backing file.
        // To avoid this, we use the persist method, which calls sync to ensure the vec is written
        // to disk, and destructs the MmapVec without deleting the backing file.

        std::mem::replace(&mut self.txs, MmapVec::new())
            .persist()
            .expect("could not persist txs");
        std::mem::replace(&mut self.blocks, MmapVec::new())
            .persist()
            .expect("could not persist blocks");
    }
}

fn open_db(db_root_path: PathBuf) -> Result<Db> {
    std::fs::create_dir_all(&db_root_path).wrap_err("Failed to create DB dir")?;

    let blocks = unsafe { MmapVec::with_name(db_root_path.join("blocks"), EXPECTED_BLOCK_COUNT) }
        .wrap_err("Failed to open blocks table")?;

    let txs = unsafe {
        MmapVec::with_name(
            db_root_path.join("txs"),
            EXPECTED_BLOCK_COUNT * EXPECTED_TXS_PER_BLOCK,
        )
    }
    .wrap_err("Failed to open txs table")?;

    Ok(Db { blocks, txs })
}

// This is a POC to show how TTOR can be validated in parallel without locks.
//
// At a high level, it works by spawning N threads that each hash and validate a different
// part of the block. This happens in two stages. In the first stage, each thread
// creates an index for its part of the block of TXID -> Position, and once complete,
// these indexes are shared with all the other threads. In stage two, validation occurs in parallel.
// Validation requires checking multiple indexes, but because we only have to look backward,
// the impact is not as bad as it might seem.
//
// Terms:
//
//      Bound - [start, end) pair defining which transactions a thread cares about
//      PTable - Position table, a hash maps of TXIDs to their index in the block
//      tid - Thread index
//
// This implementation does not do real validation, nor build the merkle root. It only demonstrates
// that the topological ordering is correct in parallel.
//
// In a real implementation, we would define the block bounds differently, since transactions
// would be different sizes, and probably not all in memory. But calculating these bounds is still
// fast (can be done while reading the block), and would be required whether CTOR or TTOR.

extern crate rand;
extern crate ring;

use rand::{random, thread_rng, Rng};
use ring::digest::{digest, SHA256};
use std::collections::HashMap;
use std::env::args;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread::spawn;
use std::time::SystemTime;

type Bounds = Arc<Vec<(usize, usize)>>;
type PTable = HashMap<[u8; 32], usize>;
type PTables = Arc<Vec<PTable>>;
type Block = Arc<Vec<u8>>;

fn main() {
    let args: Vec<String> = args().collect();
    if args.len() != 6 {
        println!("parallel_ttor <NUM_THREADS> <NUM_TXNS> <TX_SIZE> <NUM_REORDER> <PINSIDE>");
        println!("Ex. (200MB block): parallel_ttor 4 1000000 200 0 0.1");
        return;
    }
    let num_threads: usize = args[1].parse().unwrap();
    let num_txns: usize = args[2].parse().unwrap();
    let tx_size: usize = args[3].parse().unwrap();
    let num_reorder: usize = args[4].parse().unwrap();
    let p_inside: f32 = args[5].parse().unwrap();

    let block = gen_block(num_txns, tx_size, num_reorder, p_inside);
    let bounds = setup_bounds(num_threads, num_txns);
    let start_time = SystemTime::now();

    let (ptable_sndr, ptable_rcvr) = channel();
    println!("Parallel hashing");
    for tid in 0..num_threads {
        spawn_hasher(
            tid,
            block.clone(),
            bounds.clone(),
            ptable_sndr.clone(),
            tx_size,
        );
    }
    let ptables = collect_ptables(ptable_rcvr, num_threads);
    let hashing_complete_time = SystemTime::now();

    println!("Parallel TTOR validation");
    let (result_sndr, result_rcvr) = channel();
    for tid in 0..num_threads {
        spawn_validator(
            tid,
            block.clone(),
            bounds.clone(),
            ptables.clone(),
            result_sndr.clone(),
            tx_size,
        );
    }
    println!("Result: {}", aggregate_results(result_rcvr, num_threads));

    let now = SystemTime::now();
    let hashing_time = hashing_complete_time.duration_since(start_time).unwrap();
    println!("Hashing Time: {:?}", hashing_time);
    let validation_time = now.duration_since(hashing_complete_time).unwrap();
    println!("Validation Time: {:?}", validation_time);
    let total_time = now.duration_since(start_time).unwrap();
    println!("Total Time: {:?}", total_time);
}

fn setup_bounds(num_threads: usize, num_txns: usize) -> Bounds {
    let mut bounds = Vec::<(usize, usize)>::new();
    for thread in 0..num_threads {
        let low = thread * num_txns / num_threads;
        let high = (thread + 1) * num_txns / num_threads;
        bounds.push((low, high));
    }
    println!("Bounds: {:?}", bounds);
    Arc::new(bounds)
}

fn spawn_hasher(
    tid: usize,
    block: Block,
    bounds: Bounds,
    ptable_sndr: Sender<(usize, PTable)>,
    tx_size: usize,
) {
    spawn(move || {
        let mut ptable = HashMap::new();
        for n in bounds[tid].0..bounds[tid].1 {
            ptable.insert(txid(n, &block, tx_size), n);
        }
        ptable_sndr.send((tid, ptable)).unwrap();
    });
}

fn collect_ptables(ptable_rcvr: Receiver<(usize, PTable)>, num_threads: usize) -> PTables {
    let mut ptables = vec![HashMap::new(); num_threads];
    for _ in 0..num_threads {
        let (tid, ptable) = ptable_rcvr.recv().unwrap();
        ptables[tid] = ptable;
    }
    Arc::new(ptables)
}

fn spawn_validator(
    tid: usize,
    block: Block,
    bounds: Bounds,
    ptables: PTables,
    result_sndr: Sender<bool>,
    tx_size: usize,
) {
    spawn(move || {
        for n in bounds[tid].0..bounds[tid].1 {
            let tx_input = &block[n * tx_size..n * tx_size + 32];
            if tx_input != &[0; 32] {
                // This txn has an input in the block
                // Check that it exists in this ptable or one of the prior
                let mut found = false;
                for i in (0..tid + 1).rev() {
                    if let Some(pos) = ptables[i].get(tx_input) {
                        if *pos > n {
                            println!("Out of order: {} {}", n, *pos);
                            result_sndr.send(false).unwrap();
                            return;
                        } else {
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    println!("Missing input: {}", n);
                    result_sndr.send(false).unwrap();
                    return;
                }
            }
        }
        result_sndr.send(true).unwrap();
    });
}

fn aggregate_results(result_rcvr: Receiver<bool>, num_threads: usize) -> bool {
    for _ in 0..num_threads {
        if !result_rcvr.recv().unwrap() {
            return false;
        }
    }
    return true;
}

// In our model, blocks are just a single input followed by random filler
// If an input is 0s, then it represents a UTXO before this block
fn gen_block(num_txns: usize, tx_size: usize, num_reorder: usize, p_inside: f32) -> Block {
    println!("Generating block");
    let mut rng = thread_rng();
    let mut block = vec![0; num_txns * tx_size];
    for i in 0..num_txns {
        if i > 0 && random::<f32>() <= p_inside {
            let prev_index = random::<usize>() % i;
            let prev_hash = txid(prev_index, &block, tx_size);
            block[i * tx_size..i * tx_size + 32].clone_from_slice(&prev_hash);
        }
        rng.fill(&mut block[i * tx_size + 32..(i + 1) * tx_size]);
    }
    reorder_block(num_reorder, &mut block, num_txns, tx_size);
    Arc::new(block)
}

fn reorder_block(n: usize, block: &mut [u8], num_txns: usize, tx_size: usize) {
    for _ in 0..n {
        let a = (random::<usize>() % num_txns) * tx_size;
        let b = (random::<usize>() % num_txns) * tx_size;
        let a_data = block[a..a + 32].to_vec();
        let b_data = block[b..b + 32].to_vec();
        block[a..a + 32].clone_from_slice(&b_data);
        block[b..b + 32].clone_from_slice(&a_data);
    }
}

fn txid(n: usize, block: &[u8], tx_size: usize) -> [u8; 32] {
    let data = &block[n * tx_size..(n + 1) * tx_size];
    let sha256 = digest(&SHA256, &data);
    let sha256d = digest(&SHA256, sha256.as_ref());
    let mut hash256 = [0; 32];
    hash256.clone_from_slice(sha256d.as_ref());
    hash256
}

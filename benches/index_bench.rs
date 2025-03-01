use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{Rng, thread_rng};
use silentserver::storage::{Index, IndexEntry};
use std::env;
use std::fs;
use std::path::PathBuf;

const MAX_HEIGHT: usize = 100_000;

fn temp_dir(name: &str) -> PathBuf {
    let mut dir = env::temp_dir();
    dir.push(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn bench_index_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_operations");
    
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    
    group.bench_function("insert_block", |b| {
        let index_dir = temp_dir("bench_block_index");
        let (mut index, _) = Index::initialize(&index_dir).unwrap();
        
        // Pre-generate all the test data
        let mut rng = thread_rng();
        let mut blockhashes = Vec::with_capacity(MAX_HEIGHT);
        let mut entries = Vec::with_capacity(MAX_HEIGHT);
        
        for height in 0..MAX_HEIGHT as u32 {
            let mut blockhash = [0u8; 32];
            rng.fill(&mut blockhash);
            blockhashes.push(blockhash);
            
            entries.push(IndexEntry {
                file_number: (height / 1000) as u64,
                offset: (height as u64 * 1000) % 100_0000,
                length: 500,
            });
        }
        
        let mut i = 0;
        b.iter(|| {
            black_box(index.insert_block(i as u32, &blockhashes[i % MAX_HEIGHT], &entries[i % MAX_HEIGHT]).unwrap());
            i += 1;
        });
        
        let _ = fs::remove_dir_all(index_dir);
    });


    group.bench_function("random_read", |b| {
        let index_dir = temp_dir("bench_block_index_reads");
        let (mut index, _) = Index::initialize(&index_dir).unwrap();
        
        // Pre-generate test data and insert it
        let mut rng = thread_rng();
        let mut blockhashes = Vec::with_capacity(MAX_HEIGHT);
        
        for height in 0..MAX_HEIGHT as u32 {
            let mut blockhash = [0u8; 32];
            rng.fill(&mut blockhash);
            blockhashes.push(blockhash);
            
            let entry = IndexEntry {
                file_number: (height / 1000) as u64,
                offset: (height as u64 * 1000) % 100_0000,
                length: 500,
            };
            index.insert_block(height, &blockhash, &entry).unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            black_box(index.get_block_entry(&blockhashes[i % MAX_HEIGHT]).unwrap());
            i += 1;
        });

        // Cleanup
        let _ = fs::remove_dir_all(index_dir);
    });

    group.finish();
}

criterion_group!(benches, bench_index_operations);
criterion_main!(benches); 
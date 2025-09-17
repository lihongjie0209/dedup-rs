use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use jwalk::WalkDir;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use serde::Serialize;

// 定义命令行参数
#[derive(Parser, Debug)]
#[command(author, version, about = "Scan a directory and report groups of duplicate files", long_about = None)]
struct Args {
    /// The directory to scan for duplicate files
    #[arg(required = true)]
    directory: String,

    /// Optional: write results to a file
    #[arg(short = 'o', long = "output", value_name = "PATH")] 
    output: Option<String>,

    /// Output format: txt, csv, json
    #[arg(long = "format", value_enum, default_value_t = OutputFormat::Txt)]
    format: OutputFormat,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat { Txt, Csv, Json }

const PARTIAL_HASH_SIZE: usize = 4096; // 4KB from head and 4KB from tail when possible

fn main() -> io::Result<()> {
    let args = Args::parse();
    let scan_path = Path::new(&args.directory);
    let start_time = Instant::now();

    println!("Stage 1: Collecting files and grouping by size...");
    let stage1_start = Instant::now();
    let size_groups = group_by_size(scan_path);
    // Metrics: total files and bytes across all files
    let total_files: u64 = size_groups.values().map(|v| v.len() as u64).sum();
    let total_bytes: u64 = size_groups.iter().map(|(sz, v)| (*sz as u64) * (v.len() as u64)).sum();

    let potential_duplicates: HashMap<u64, Vec<PathBuf>> = size_groups
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .collect();
    let candidate_groups = potential_duplicates.len() as u64;
    let time_stage1 = stage1_start.elapsed();
    println!(
        "Found {} groups of files with identical sizes. Time elapsed: {:.2?}",
        potential_duplicates.len(),
        start_time.elapsed()
    );

    println!("\nStage 2: Filtering by partial hash...");
    let stage2_start = Instant::now();
    // Metrics: partial bytes hashed (per file: up to head+tail)
    let bytes_hashed_partial: u64 = potential_duplicates
        .iter()
        .map(|(sz, paths)| {
            let per_file = usize::min(*sz as usize, 2 * PARTIAL_HASH_SIZE) as u64;
            per_file * (paths.len() as u64)
        })
        .sum();
    let partial_hash_groups = filter_by_partial_hash(potential_duplicates);
    let partial_groups = partial_hash_groups.len() as u64;
    let time_stage2 = stage2_start.elapsed();
    println!(
        "Found {} groups after partial hash check. Time elapsed: {:.2?}",
        partial_hash_groups.len(),
        start_time.elapsed()
    );

    println!("\nStage 3: Confirming with full hash...");
    // Metrics: bytes hashed fully (sum of sizes for all files entering stage 3)
    let bytes_hashed_full: u64 = partial_hash_groups
        .values()
        .flatten()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    let stage3_start = Instant::now();
    let duplicate_groups = confirm_with_full_hash(partial_hash_groups);
    let time_stage3 = stage3_start.elapsed();
    println!(
        "Found {} groups of duplicate files. Total time: {:.2?}",
        duplicate_groups.len(),
        start_time.elapsed()
    );

    // Metrics based on results
    let duplicate_files: u64 = duplicate_groups.iter().map(|g| g.len() as u64).sum();
    let reclaimable_bytes: u64 = duplicate_groups
        .iter()
        .map(|g| {
            if g.is_empty() { return 0; }
            let size = std::fs::metadata(&g[0]).map(|m| m.len()).unwrap_or(0);
            size.saturating_mul((g.len().saturating_sub(1)) as u64)
        })
        .sum();

    let metrics = Metrics {
        total_files,
        total_bytes,
        candidate_groups,
        partial_groups,
        duplicate_groups: duplicate_groups.len() as u64,
        duplicate_files,
        reclaimable_bytes,
        bytes_hashed_partial,
        bytes_hashed_full,
        time_stage1_secs: dur_secs(time_stage1),
        time_stage2_secs: dur_secs(time_stage2),
        time_stage3_secs: dur_secs(time_stage3),
        time_total_secs: dur_secs(start_time.elapsed()),
    };

    println!("\n--- Duplicate Files Found ---");
    if let Some(out_path) = &args.output {
        write_output(out_path, args.format, &duplicate_groups, &metrics)?;
        println!("Results written to {} in {:?} format.", out_path, args.format);
    } else {
        print_output(args.format, &duplicate_groups, &metrics)?;
    }

    Ok(())
}

fn dur_secs(d: Duration) -> f64 { d.as_secs_f64() }

/// Stage 1: 遍历目录，按文件大小分组
fn group_by_size(path: &Path) -> HashMap<u64, Vec<PathBuf>> {
    use rayon::iter::ParallelBridge;
    let spinner = ProgressBar::new_spinner();
    spinner.set_message("Scanning files (parallel)...");

    // jwalk yields entries (iterator). Use par_bridge to process entries in parallel.
    let entries: Vec<(u64, PathBuf)> = WalkDir::new(path)
        .into_iter()
        .par_bridge()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            spinner.tick();
            let meta_res = e.metadata().map_err(|_| ()).or_else(|_| {
                std::fs::metadata(e.path()).map_err(|_| ())
            });
            match meta_res {
                Ok(md) if md.len() > 0 => Some((md.len(), e.path())),
                _ => None,
            }
        })
        .collect();

    spinner.finish_with_message("Done scanning.");

    let mut size_map: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    for (len, path) in entries {
        size_map.entry(len).or_default().push(path);
    }
    size_map
}

/// Stage 2: 对大小相同的文件组计算部分哈希值
fn filter_by_partial_hash(
    size_groups: HashMap<u64, Vec<PathBuf>>,
) -> HashMap<String, Vec<PathBuf>> {
    let total: u64 = size_groups.values().map(|v| v.len() as u64).sum();
    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap(),
    );

    // 计算 (partial_hash, path) 对，然后按 hash 分组
    let pairs: Vec<(String, PathBuf)> = size_groups
        .into_par_iter()
        .flat_map(|(_, paths)| {
            let mut v = Vec::with_capacity(paths.len());
            for path in paths {
                bar.inc(1);
                if let Ok(hash) = calculate_partial_hash(&path) {
                    v.push((hash, path));
                }
            }
            v
        })
        .collect();

    let mut hash_groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (hash, path) in pairs {
        hash_groups.entry(hash).or_default().push(path);
    }
    // 仅保留存在多个文件的 hash 组
    hash_groups.retain(|_, v| v.len() > 1);

    bar.finish_with_message("Partial hash check complete.");
    hash_groups
}

/// Stage 3: 对部分哈希值也相同的文件组计算完整哈希值
fn confirm_with_full_hash(
    partial_hash_groups: HashMap<String, Vec<PathBuf>>,
) -> Vec<Vec<PathBuf>> {
    let total: u64 = partial_hash_groups.values().map(|v| v.len() as u64).sum();
    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap(),
    );

    // 将所有路径拍平后并行计算完整哈希
    let all_paths: Vec<PathBuf> = partial_hash_groups
        .into_values()
        .flatten()
        .collect();

    let pairs: Vec<(String, PathBuf)> = all_paths
        .into_par_iter()
        .filter_map(|path| {
            bar.inc(1);
            match calculate_full_hash(&path) {
                Ok(h) => Some((h, path)),
                Err(_) => None,
            }
        })
        .collect();

    let mut full_hash_groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (hash, path) in pairs {
        full_hash_groups.entry(hash).or_default().push(path);
    }

    let duplicate_groups: Vec<Vec<PathBuf>> = full_hash_groups
        .into_values()
        .filter(|v| v.len() > 1)
        .collect();

    bar.finish_with_message("Full hash check complete.");
    duplicate_groups
}

/// 计算文件前 `PARTIAL_HASH_SIZE` 字节的 BLAKE3 哈希
fn calculate_partial_hash(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    let len = metadata.len();

    // Read head up to PARTIAL_HASH_SIZE
    let mut head_buf = vec![0u8; PARTIAL_HASH_SIZE];
    let mut head_read = 0usize;
    {
        let mut reader = BufReader::new(&file);
        head_read = reader.read(&mut head_buf)?;
        head_buf.truncate(head_read);
    }

    // Read tail up to PARTIAL_HASH_SIZE (if file is larger than head read)
    let mut tail_buf = Vec::new();
    if len as usize > head_read {
        let tail_size = PARTIAL_HASH_SIZE.min((len as usize).saturating_sub(head_read));
        file.seek(SeekFrom::End(-(tail_size as i64)))?;
        let mut reader = BufReader::new(&file);
        tail_buf.resize(tail_size, 0);
        let read_tail = reader.read(&mut tail_buf)?;
        tail_buf.truncate(read_tail);
    }

    // Combine head and tail into one hash
    let mut hasher = blake3::Hasher::new();
    hasher.update(&head_buf);
    hasher.update(&tail_buf);
    Ok(hasher.finalize().to_hex().to_string())
}

/// 计算整个文件的 BLAKE3 哈希
fn calculate_full_hash(path: &Path) -> io::Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0; 65536]; // 64KB buffer

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Serialize)]
struct JsonOutputGroup {
    group: usize,
    files: Vec<String>,
}

#[derive(Serialize)]
struct Metrics {
    total_files: u64,
    total_bytes: u64,
    candidate_groups: u64,
    partial_groups: u64,
    duplicate_groups: u64,
    duplicate_files: u64,
    reclaimable_bytes: u64,
    bytes_hashed_partial: u64,
    bytes_hashed_full: u64,
    time_stage1_secs: f64,
    time_stage2_secs: f64,
    time_stage3_secs: f64,
    time_total_secs: f64,
}

#[derive(Serialize)]
struct CombinedJsonOutput<'a> {
    metrics: &'a Metrics,
    groups: &'a [JsonOutputGroup],
}

fn print_output(fmt: OutputFormat, groups: &Vec<Vec<PathBuf>>, metrics: &Metrics) -> io::Result<()> {
    match fmt {
        OutputFormat::Txt => {
            if groups.is_empty() {
                println!("No duplicate files found.");
            } else {
                for (i, group) in groups.iter().enumerate() {
                    println!("\nGroup {}:", i + 1);
                    for p in group {
                        println!("  - {}", p.display());
                    }
                }
            }
            println!("\n=== Metrics ===");
            print_metrics_txt(metrics);
        }
        OutputFormat::Csv => {
            println!("group,path");
            for (i, group) in groups.iter().enumerate() {
                for p in group {
                    let path = p.display().to_string();
                    println!("{},{}", i + 1, csv_escape(&path));
                }
            }
            println!("");
            println!("metric,value");
            for (k, v) in metrics_kv(metrics) {
                println!("{},{}", k, v);
            }
        }
        OutputFormat::Json => {
            let json_groups: Vec<JsonOutputGroup> = groups
                .iter()
                .enumerate()
                .map(|(i, g)| JsonOutputGroup {
                    group: i + 1,
                    files: g.iter().map(|p| p.display().to_string()).collect(),
                })
                .collect();
            let combined = CombinedJsonOutput { metrics, groups: &json_groups };
            let s = serde_json::to_string_pretty(&combined).unwrap();
            println!("{}", s);
        }
    }
    Ok(())
}

fn write_output(path: &str, fmt: OutputFormat, groups: &Vec<Vec<PathBuf>>, metrics: &Metrics) -> io::Result<()> {
    let mut f = File::create(path)?;
    match fmt {
        OutputFormat::Txt => {
            if groups.is_empty() {
                f.write_all(b"No duplicate files found.\n")?;
            } else {
                for (i, group) in groups.iter().enumerate() {
                    writeln!(f, "\nGroup {}:", i + 1)?;
                    for p in group {
                        writeln!(f, "  - {}", p.display())?;
                    }
                }
            }
            writeln!(f, "\n=== Metrics ===")?;
            write_metrics_txt(&mut f, metrics)?;
        }
        OutputFormat::Csv => {
            writeln!(f, "group,path")?;
            for (i, group) in groups.iter().enumerate() {
                for p in group {
                    let path = p.display().to_string();
                    writeln!(f, "{},{}", i + 1, csv_escape(&path))?;
                }
            }
            writeln!(f, "")?;
            writeln!(f, "metric,value")?;
            for (k, v) in metrics_kv(metrics) {
                writeln!(f, "{},{}", k, v)?;
            }
        }
        OutputFormat::Json => {
            let json_groups: Vec<JsonOutputGroup> = groups
                .iter()
                .enumerate()
                .map(|(i, g)| JsonOutputGroup {
                    group: i + 1,
                    files: g.iter().map(|p| p.display().to_string()).collect(),
                })
                .collect();
            let combined = CombinedJsonOutput { metrics, groups: &json_groups };
            let s = serde_json::to_string_pretty(&combined).unwrap();
            f.write_all(s.as_bytes())?;
            f.write_all(b"\n")?;
        }
    }
    Ok(())
}

fn csv_escape(s: &str) -> String {
    let needs_quote = s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r');
    if !needs_quote {
        return s.to_string();
    }
    let escaped = s.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn print_metrics_txt(m: &Metrics) {
    println!("total_files: {}", m.total_files);
    println!("total_bytes: {}", m.total_bytes);
    println!("candidate_groups: {}", m.candidate_groups);
    println!("partial_groups: {}", m.partial_groups);
    println!("duplicate_groups: {}", m.duplicate_groups);
    println!("duplicate_files: {}", m.duplicate_files);
    println!("reclaimable_bytes: {}", m.reclaimable_bytes);
    println!("bytes_hashed_partial: {}", m.bytes_hashed_partial);
    println!("bytes_hashed_full: {}", m.bytes_hashed_full);
    println!("time_stage1_secs: {:.3}", m.time_stage1_secs);
    println!("time_stage2_secs: {:.3}", m.time_stage2_secs);
    println!("time_stage3_secs: {:.3}", m.time_stage3_secs);
    println!("time_total_secs: {:.3}", m.time_total_secs);
}

fn write_metrics_txt(w: &mut impl Write, m: &Metrics) -> io::Result<()> {
    writeln!(w, "total_files: {}", m.total_files)?;
    writeln!(w, "total_bytes: {}", m.total_bytes)?;
    writeln!(w, "candidate_groups: {}", m.candidate_groups)?;
    writeln!(w, "partial_groups: {}", m.partial_groups)?;
    writeln!(w, "duplicate_groups: {}", m.duplicate_groups)?;
    writeln!(w, "duplicate_files: {}", m.duplicate_files)?;
    writeln!(w, "reclaimable_bytes: {}", m.reclaimable_bytes)?;
    writeln!(w, "bytes_hashed_partial: {}", m.bytes_hashed_partial)?;
    writeln!(w, "bytes_hashed_full: {}", m.bytes_hashed_full)?;
    writeln!(w, "time_stage1_secs: {:.3}", m.time_stage1_secs)?;
    writeln!(w, "time_stage2_secs: {:.3}", m.time_stage2_secs)?;
    writeln!(w, "time_stage3_secs: {:.3}", m.time_stage3_secs)?;
    writeln!(w, "time_total_secs: {:.3}", m.time_total_secs)?;
    Ok(())
}

fn metrics_kv(m: &Metrics) -> Vec<(&'static str, String)> {
    vec![
        ("total_files", m.total_files.to_string()),
        ("total_bytes", m.total_bytes.to_string()),
        ("candidate_groups", m.candidate_groups.to_string()),
        ("partial_groups", m.partial_groups.to_string()),
        ("duplicate_groups", m.duplicate_groups.to_string()),
        ("duplicate_files", m.duplicate_files.to_string()),
        ("reclaimable_bytes", m.reclaimable_bytes.to_string()),
        ("bytes_hashed_partial", m.bytes_hashed_partial.to_string()),
        ("bytes_hashed_full", m.bytes_hashed_full.to_string()),
        ("time_stage1_secs", format!("{:.3}", m.time_stage1_secs)),
        ("time_stage2_secs", format!("{:.3}", m.time_stage2_secs)),
        ("time_stage3_secs", format!("{:.3}", m.time_stage3_secs)),
        ("time_total_secs", format!("{:.3}", m.time_total_secs)),
    ]
}

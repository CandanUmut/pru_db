use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rand::Rng;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pru_core::{
    consts::SegmentKind,
    manifest::Manifest,
    postings::{decode_sorted_u64, encode_sorted_u64, merge_sorted},
    resolver_store::{ResolveMode, ResolverStore},
    segment::{SegmentReader, SegmentWriter},
    Fact, PruStore,
};

#[derive(Parser)]
#[command(name = "pru", about = "PRU-DB CLI — core ops")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(ValueEnum, Clone)]
enum CliResolveMode {
    Union,
    Dedup,
    Intersect,
}

#[derive(Subcommand)]
enum Cmd {
    Init {
        #[arg(long)]
        dir: PathBuf,
    },

    AddResolver {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        key_hex: String,
        #[arg(long, num_args=1.., value_delimiter=',')]
        ids: Vec<u64>,
    },

    Resolve {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        key_hex: String,
        /// Optional extra keys for intersect/union
        #[arg(long, value_name="HEX", num_args=0.., value_delimiter=',')]
        and_key_hex: Vec<String>,
        /// union (default), dedup, intersect
        #[arg(long, value_enum, default_value_t=CliResolveMode::Union)]
        mode: CliResolveMode,
        /// intersect'i set-kesişimi gibi uygula (operandları önce dedup et)
        #[arg(long, default_value_t = false)]
        set: bool,
    },

    Verify {
        #[arg(long)]
        dir: PathBuf,
    },

    Compact {
        #[arg(long)]
        dir: PathBuf,
    },

    Promote {
        #[arg(long)]
        dir: PathBuf,
    },

    Info {
        #[arg(long)]
        dir: PathBuf,
    },

    Entity {
        #[command(subcommand)]
        cmd: EntityCmd,
    },

    Predicate {
        #[command(subcommand)]
        cmd: PredicateCmd,
    },

    Literal {
        #[command(subcommand)]
        cmd: LiteralCmd,
    },

    Fact {
        #[command(subcommand)]
        cmd: FactCmd,
    },
}

#[derive(Subcommand)]
enum EntityCmd {
    Add {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum PredicateCmd {
    Add {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum LiteralCmd {
    Add {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        value: String,
    },
}

#[derive(Subcommand)]
enum FactCmd {
    Add {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, value_name = "ID")]
        subject_id: u64,
        #[arg(long, value_name = "ID")]
        predicate_id: u64,
        #[arg(long, value_name = "ID")]
        object_id: u64,
        #[arg(long, value_name = "ID")]
        source_id: Option<u64>,
        #[arg(long)]
        timestamp: Option<i64>,
    },
    List {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, value_name = "ID")]
        subject_id: u64,
        #[arg(long, value_name = "ID")]
        predicate_id: Option<u64>,
    },
}

fn ensure_dir(p: &Path) -> Result<()> {
    std::fs::create_dir_all(p)?;
    Ok(())
}

fn now_ts() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

fn now_id() -> String {
    let now = time::OffsetDateTime::now_utc();
    let secs = now.unix_timestamp();
    let nanos = now.nanosecond();
    let mut rng = rand::thread_rng();
    let r: u16 = rng.gen();
    format!("{secs}-{nanos:09}-{r:04x}")
}

fn open_store(dir: &Path) -> Result<PruStore> {
    ensure_dir(dir)?;
    Ok(PruStore::open(dir)?)
}

fn render_atom(store: &PruStore, id: u64) -> String {
    if let Some(name) = store.get_entity_name(id) {
        return format!("{name} [entity #{id}]");
    }
    if let Some(name) = store.get_predicate_name(id) {
        return format!("{name} [predicate #{id}]");
    }
    if let Some(val) = store.get_literal_value(id) {
        return format!("{val} [literal #{id}]");
    }
    format!("#{id}")
}

fn print_fact(store: &PruStore, fact: &Fact) {
    let subj = render_atom(store, fact.subject);
    let pred = render_atom(store, fact.predicate);
    let obj = render_atom(store, fact.object);
    let ts = fact
        .timestamp
        .map(|t| format!(" @{}", t))
        .unwrap_or_default();
    let src = fact
        .source
        .map(|s| format!(" source={}", s))
        .unwrap_or_default();
    println!("{subj} --{pred}--> {obj}{src}{ts}");
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init { dir } => {
            ensure_dir(&dir)?;
            let m = Manifest::load(&dir)?;
            m.save_atomic(&dir)?;
            println!("init: {}", dir.display());
        }
        Cmd::AddResolver { dir, key_hex, ids } => {
            ensure_dir(&dir)?;
            let key = hex::decode(key_hex)?;
            let seg_name = format!("resolver-{}.prus", now_ts());
            let seg_path = dir.join(&seg_name);

            let mut w = SegmentWriter::create(&seg_path, SegmentKind::Resolver, 1 << 20, 7)?;
            let mut lst = ids;
            lst.sort_unstable();
            lst.dedup();
            w.add(&key, &encode_sorted_u64(&lst))?;
            w.finalize()?;

            let mut man = Manifest::load(&dir)?;
            man.add_segment(&dir, &seg_name, SegmentKind::Resolver)?;
            man.save_atomic(&dir)?;
            println!("added segment: {}", seg_name);
        }
        Cmd::Resolve {
            dir,
            key_hex,
            and_key_hex,
            mode,
            set,
        } => {
            let first = hex::decode(key_hex)?;
            let mut keys: Vec<Vec<u8>> = vec![first];
            for h in and_key_hex {
                keys.push(hex::decode(h)?);
            }
            let store = ResolverStore::open(&dir)?;
            let m = match mode {
                CliResolveMode::Union => ResolveMode::Union,
                CliResolveMode::Dedup => ResolveMode::Dedup,
                CliResolveMode::Intersect => ResolveMode::Intersect,
            };
            let out = store.resolve_with_mode_set(m, &keys, set);
            println!("{:?}", out);
        }
        Cmd::Verify { dir } => {
            let man = Manifest::load(&dir)?;
            let mut seg_ok = 0usize;
            let mut seg_fail = 0usize;
            let mut total = 0usize;
            let mut bad_bounds = 0usize;
            let mut bad_crc = 0usize;
            let mut filter_miss = 0usize;
            let mut total_slots: u64 = 0;
            let mut total_filled: u64 = 0;

            for s in &man.segments {
                let path = dir.join(&s.path);
                match SegmentReader::open(&path) {
                    Ok(r) => {
                        if let Some((_k, cap)) = r.index_meta() {
                            total_slots += cap;
                        }
                        if r.kind == SegmentKind::Resolver {
                            let mut filled_here: u64 = 0;
                            for e in r.iter() {
                                filled_here += 1;
                                total += 1;
                                let end = (e.off as usize).saturating_add(e.size as usize);
                                if end > std::fs::metadata(&path)?.len() as usize || e.size < 4 {
                                    bad_bounds += 1;
                                    continue;
                                }
                                if !r.verify_crc_at(e.off as usize, e.size as usize) {
                                    bad_crc += 1;
                                }
                                if let Some(hit) = r.filter_contains_digest(e.hash) {
                                    if !hit {
                                        filter_miss += 1;
                                    }
                                }
                            }
                            total_filled += filled_here;
                        }
                        seg_ok += 1;
                    }
                    Err(e) => {
                        eprintln!("verify: failed to open {}: {e}", path.display());
                        seg_fail += 1;
                    }
                }
            }
            let lf = if total_slots > 0 {
                (total_filled as f64) / (total_slots as f64)
            } else {
                0.0
            };
            println!("verify: segments ok={}, fail={}", seg_ok, seg_fail);
            println!(
                "         entries={}  bad_bounds={}  bad_crc={}  filter_miss(XOR)={}",
                total, bad_bounds, bad_crc, filter_miss
            );
            println!(
                "         load_factor(avg)≈{:.2} (filled={total_filled} / slots={total_slots})",
                lf
            );
        }
        Cmd::Compact { dir } => {
            let man = Manifest::load(&dir)?;
            let mut mp: HashMap<u64, Vec<u64>> = HashMap::new();
            let mut input_segments = 0usize;
            for s in &man.segments {
                if s.kind != SegmentKind::Resolver {
                    continue;
                }
                let r = SegmentReader::open(dir.join(&s.path))?;
                input_segments += 1;
                for e in r.iter() {
                    if let Some(val) = r.value_at(e.off as usize, e.size as usize) {
                        let mut lst = decode_sorted_u64(val);
                        if lst.is_empty() {
                            continue;
                        }
                        lst.sort_unstable();
                        lst.dedup();
                        mp.entry(e.hash)
                            .and_modify(|acc| {
                                let merged = merge_sorted(acc, &lst);
                                *acc = merged;
                            })
                            .or_insert(lst);
                    }
                }
            }
            if input_segments == 0 {
                return Err(anyhow!("no resolver segments to compact"));
            }

            // Çakışma guard: nano + random
            let seg_name = format!("resolver-compact-{}.prus", now_id());
            let seg_path = dir.join(&seg_name);
            let mut w = SegmentWriter::create(&seg_path, SegmentKind::Resolver, 1 << 20, 7)?;
            w.set_index_kind(pru_core::consts::INDEX_KIND_HASHTAB); // V1
            w.set_filter_xor8();

            let mut keys: Vec<u64> = mp.keys().copied().collect();
            keys.sort_unstable();
            for h in keys {
                let enc = encode_sorted_u64(mp.get(&h).unwrap());
                w.add_hashed(h, &enc)?;
            }
            w.finalize()?;

            let mut man2 = Manifest::load(&dir)?;
            man2.add_segment(&dir, &seg_name, SegmentKind::Resolver)?;
            man2.save_atomic(&dir)?;
            println!("compact: wrote {}, entries={}", seg_name, mp.len());
        }
        Cmd::Promote { dir } => {
            let mut man = Manifest::load(&dir)?;
            let changed = man.promote_resolver_compact()?;
            man.save_atomic(&dir)?;
            println!("promote: active set updated (resolver active={})", changed);
            println!("active:  {:?}", man.active_paths);
            if !man.archived_paths.is_empty() {
                println!("archived: {:?}", man.archived_paths);
            }
        }
        Cmd::Info { dir } => {
            let man = Manifest::load(&dir)?;
            println!("segments: {}", man.segments.len());
            let act = man.active_segment_paths();
            println!("active   : {}", act.len());
            for s in &man.segments {
                let full = dir.join(&s.path);
                let mark = if act.iter().any(|p| *p == s.path) {
                    '*'
                } else {
                    ' '
                };
                let mut extra = String::new();
                if let Ok(r) = SegmentReader::open(&full) {
                    if let Some((k, cap)) = r.index_meta() {
                        let filled = r.iter().count();
                        let lf = if cap > 0 {
                            (filled as f64) / (cap as f64)
                        } else {
                            0.0
                        };
                        extra =
                            format!(" entries={} cap={} load≈{:.2} kind={}", filled, cap, lf, k);
                    }
                }
                println!(
                    "{} {:?} {}{}",
                    mark,
                    s.kind,
                    s.path.display(),
                    if extra.is_empty() {
                        String::new()
                    } else {
                        format!("  [{}]", extra)
                    }
                );
            }
        }

        Cmd::Entity { cmd } => match cmd {
            EntityCmd::Add { dir, name } => {
                let mut store = open_store(&dir)?;
                let id = store.intern_entity(&name)?;
                println!("entity added: {name} -> #{id}");
            }
        },

        Cmd::Predicate { cmd } => match cmd {
            PredicateCmd::Add { dir, name } => {
                let mut store = open_store(&dir)?;
                let id = store.intern_predicate(&name)?;
                println!("predicate added: {name} -> #{id}");
            }
        },

        Cmd::Literal { cmd } => match cmd {
            LiteralCmd::Add { dir, value } => {
                let mut store = open_store(&dir)?;
                let id = store.intern_literal(&value)?;
                println!("literal added: {value} -> #{id}");
            }
        },

        Cmd::Fact { cmd } => match cmd {
            FactCmd::Add {
                dir,
                subject_id,
                predicate_id,
                object_id,
                source_id,
                timestamp,
            } => {
                let mut store = open_store(&dir)?;
                let fact = Fact {
                    subject: subject_id,
                    predicate: predicate_id,
                    object: object_id,
                    source: source_id,
                    timestamp,
                };
                store.add_fact(fact)?;
                println!("fact appended for subject #{subject_id}");
            }
            FactCmd::List {
                dir,
                subject_id,
                predicate_id,
            } => {
                let store = open_store(&dir)?;
                let facts = if let Some(pred) = predicate_id {
                    store.facts_for_subject_predicate(subject_id, pred)?
                } else {
                    store.facts_for_subject(subject_id)?
                };
                if facts.is_empty() {
                    println!("no facts found");
                }
                for f in &facts {
                    print_fact(&store, f);
                }
            }
        },
    }
    Ok(())
}

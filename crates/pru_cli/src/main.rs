use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use rand::Rng;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use pru_core::{
    consts::SegmentKind,
    manifest::Manifest,
    postings::{decode_sorted_u64, encode_sorted_u64, merge_sorted},
    resolver_store::{ResolveMode, ResolverStore},
    segment::{SegmentReader, SegmentWriter},
    Fact, PruStore, Query,
};

#[derive(Parser)]
#[command(
    name = "pru",
    about = "PRU-DB CLI — core ops",
    long_about = "High-level CLI for PRU-DB. Manage atoms (entities, predicates, literals), facts, and low-level segments."
)]
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
    /// Initialize a PRU-DB directory (creates manifest and tables)
    Init {
        #[arg(long, value_name = "DIR", help = "Data directory to initialize")]
        dir: PathBuf,
    },

    /// Add a resolver segment from a hex key and id list
    AddResolver {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        #[arg(long, value_name = "HEX", help = "Resolver key (hex-encoded)")]
        key_hex: String,
        #[arg(long, num_args = 1.., value_delimiter = ',', value_name = "ID")]
        ids: Vec<u64>,
    },

    /// Resolve ids using resolver segments
    Resolve {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        #[arg(long, value_name = "HEX", help = "Primary resolver key (hex)")]
        key_hex: String,
        /// Optional extra keys for intersect/union
        #[arg(long, value_name = "HEX", num_args = 0.., value_delimiter = ',')]
        and_key_hex: Vec<String>,
        /// union (default), dedup, intersect
        #[arg(long, value_enum, default_value_t = CliResolveMode::Union)]
        mode: CliResolveMode,
        /// Apply set-like intersection semantics after deduplication
        #[arg(long, default_value_t = false)]
        set: bool,
    },

    /// Verify segments on disk
    Verify {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },

    /// Compact resolver segments
    Compact {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },

    /// Promote a compacted resolver segment to active
    Promote {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },

    /// Inspect manifest and segments
    Info {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },

    /// Entity dictionary operations
    Entity {
        #[command(subcommand)]
        cmd: EntityCmd,
    },

    /// Predicate dictionary operations
    Predicate {
        #[command(subcommand)]
        cmd: PredicateCmd,
    },

    /// Literal dictionary operations
    Literal {
        #[command(subcommand)]
        cmd: LiteralCmd,
    },

    /// Fact operations
    Fact {
        #[command(subcommand)]
        cmd: FactCmd,
    },

    /// Run an ad-hoc fact query
    Query(QueryCmd),
}

#[derive(Subcommand)]
enum EntityCmd {
    /// Intern a new entity name
    Add {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        #[arg(long, value_name = "NAME")]
        name: String,
    },
    /// List all known entities
    List {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum PredicateCmd {
    /// Intern a new predicate name
    Add {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        #[arg(long, value_name = "NAME")]
        name: String,
    },
    /// List all predicates
    List {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum LiteralCmd {
    /// Intern a new literal value
    Add {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
        #[arg(long, value_name = "VALUE")]
        value: String,
    },
    /// List all literals
    List {
        #[arg(long, value_name = "DIR")]
        dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum FactCmd {
    /// Append a fact with optional metadata
    Add(FactAddCmd),
    /// List facts for a subject (optionally filtered by predicate)
    List(FactListCmd),
    /// Run a query with optional filters
    Query(QueryCmd),
}

#[derive(Args)]
struct FactAddCmd {
    #[arg(long, value_name = "DIR")]
    dir: PathBuf,
    #[arg(long, value_name = "ID", help = "Subject id")]
    subject_id: Option<u64>,
    #[arg(long, value_name = "NAME", help = "Subject name (entity)")]
    subject: Option<String>,
    #[arg(long, value_name = "ID", help = "Predicate id")]
    predicate_id: Option<u64>,
    #[arg(long, value_name = "NAME", help = "Predicate name")]
    predicate: Option<String>,
    #[arg(long, value_name = "ID", help = "Object id (entity or literal)")]
    object_id: Option<u64>,
    #[arg(long, value_name = "VALUE", help = "Object literal or entity name")]
    object: Option<String>,
    #[arg(long, value_name = "ID")]
    source_id: Option<u64>,
    #[arg(long)]
    timestamp: Option<i64>,
    #[arg(long, value_name = "FLOAT", help = "Confidence score (default 1.0)")]
    confidence: Option<f32>,
    #[arg(
        long,
        default_value_t = false,
        help = "Render facts in a human-readable form"
    )]
    pretty: bool,
}

#[derive(Args)]
struct FactListCmd {
    #[arg(long, value_name = "DIR")]
    dir: PathBuf,
    #[arg(long, value_name = "ID")]
    subject_id: Option<u64>,
    #[arg(long, value_name = "NAME")]
    subject: Option<String>,
    #[arg(long, value_name = "ID")]
    predicate_id: Option<u64>,
    #[arg(long, value_name = "NAME")]
    predicate: Option<String>,
    #[arg(long, default_value_t = false)]
    pretty: bool,
}

#[derive(Args, Clone)]
struct QueryCmd {
    #[arg(long, value_name = "DIR")]
    dir: PathBuf,
    #[arg(long, value_name = "ID")]
    subject_id: Option<u64>,
    #[arg(long, value_name = "NAME")]
    subject: Option<String>,
    #[arg(long, value_name = "ID")]
    predicate_id: Option<u64>,
    #[arg(long, value_name = "NAME")]
    predicate: Option<String>,
    #[arg(long, value_name = "ID")]
    object_id: Option<u64>,
    #[arg(long, value_name = "VALUE")]
    object: Option<String>,
    #[arg(long, value_name = "FLOAT")]
    min_confidence: Option<f32>,
    #[arg(long, default_value_t = false)]
    pretty: bool,
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
    let mut rng = rand::rng();
    let r: u16 = rng.random();
    format!("{secs}-{nanos:09}-{r:04x}")
}

fn open_store(dir: &Path) -> Result<PruStore> {
    ensure_dir(dir)?;
    PruStore::open(dir).with_context(|| format!("failed to open store at {}", dir.display()))
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

fn fact_line(store: &PruStore, fact: &Fact, pretty: bool) -> String {
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
    let conf = fact
        .confidence
        .map(|c| format!(" conf={:.2}", c))
        .unwrap_or_default();

    if pretty {
        let s_name = store
            .get_entity_name(fact.subject)
            .unwrap_or_else(|| format!("#{}", fact.subject));
        let p_name = store
            .get_predicate_name(fact.predicate)
            .unwrap_or_else(|| format!("#{}", fact.predicate));
        let o_name = store
            .get_entity_name(fact.object)
            .or_else(|| store.get_literal_value(fact.object))
            .unwrap_or_else(|| format!("#{}", fact.object));
        return format!("{s_name} {p_name} {o_name}{src}{conf}{ts}");
    }

    format!("{subj} --{pred}--> {obj}{src}{conf}{ts}")
}

fn print_fact(store: &PruStore, fact: &Fact, pretty: bool) {
    println!("{}", fact_line(store, fact, pretty));
}

fn resolve_entity(store: &PruStore, id: Option<u64>, name: Option<String>) -> Result<u64> {
    match (id, name) {
        (Some(i), None) => Ok(i),
        (None, Some(n)) => store
            .get_entity_id(&n)
            .ok_or_else(|| anyhow!("Entity not found for name: {n}")),
        (Some(_), Some(_)) => Err(anyhow!(
            "Specify either --subject-id or --subject, not both"
        )),
        (None, None) => Err(anyhow!(
            "Subject is required (use --subject-id or --subject)"
        )),
    }
}

fn resolve_predicate(store: &PruStore, id: Option<u64>, name: Option<String>) -> Result<u64> {
    match (id, name) {
        (Some(i), None) => Ok(i),
        (None, Some(n)) => store
            .get_predicate_id(&n)
            .ok_or_else(|| anyhow!("Predicate not found for name: {n}")),
        (Some(_), Some(_)) => Err(anyhow!(
            "Specify either --predicate-id or --predicate, not both"
        )),
        (None, None) => Err(anyhow!(
            "Predicate is required (use --predicate-id or --predicate)"
        )),
    }
}

fn resolve_object(store: &PruStore, id: Option<u64>, name: Option<String>) -> Result<u64> {
    match (id, name) {
        (Some(i), None) => Ok(i),
        (None, Some(n)) => store
            .get_literal_id(&n)
            .or_else(|| store.get_entity_id(&n))
            .ok_or_else(|| anyhow!("Object not found for value/name: {n}")),
        (Some(_), Some(_)) => Err(anyhow!("Specify either --object-id or --object, not both")),
        (None, None) => Err(anyhow!("Object is required (use --object-id or --object)")),
    }
}

fn handle_fact_list(store: &PruStore, args: FactListCmd) -> Result<()> {
    let subject = resolve_entity(store, args.subject_id, args.subject)?;
    let facts = if let Some(pred) = args.predicate_id {
        store.facts_for_subject_predicate(subject, pred)?
    } else if let Some(pred_name) = args.predicate {
        let pid = resolve_predicate(store, None, Some(pred_name))?;
        store.facts_for_subject_predicate(subject, pid)?
    } else {
        store.facts_for_subject(subject)?
    };

    if facts.is_empty() {
        println!("no facts found");
        return Ok(());
    }
    for f in &facts {
        print_fact(store, f, args.pretty);
    }
    Ok(())
}

fn handle_query(store: &PruStore, args: QueryCmd) -> Result<()> {
    let subject = match (args.subject_id, args.subject) {
        (None, None) => None,
        (id, name) => Some(resolve_entity(store, id, name)?),
    };
    let predicate = match (args.predicate_id, args.predicate) {
        (None, None) => None,
        (id, name) => Some(resolve_predicate(store, id, name)?),
    };
    let object = match (args.object_id, args.object) {
        (None, None) => None,
        (id, name) => Some(resolve_object(store, id, name)?),
    };

    let query = Query {
        subject,
        predicate,
        object,
        min_confidence: args.min_confidence,
    };
    let res = store.query(query)?;
    if res.is_empty() {
        println!("no facts matched query");
    }
    for f in &res {
        print_fact(store, f, args.pretty);
    }
    Ok(())
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
            EntityCmd::List { dir } => {
                let store = open_store(&dir)?;
                let entities = store.entities();
                if entities.is_empty() {
                    println!("no entities found");
                }
                for (id, name) in entities {
                    println!("#{id}\t{name}");
                }
            }
        },

        Cmd::Predicate { cmd } => match cmd {
            PredicateCmd::Add { dir, name } => {
                let mut store = open_store(&dir)?;
                let id = store.intern_predicate(&name)?;
                println!("predicate added: {name} -> #{id}");
            }
            PredicateCmd::List { dir } => {
                let store = open_store(&dir)?;
                let preds = store.predicates();
                if preds.is_empty() {
                    println!("no predicates found");
                }
                for (id, name) in preds {
                    println!("#{id}\t{name}");
                }
            }
        },

        Cmd::Literal { cmd } => match cmd {
            LiteralCmd::Add { dir, value } => {
                let mut store = open_store(&dir)?;
                let id = store.intern_literal(&value)?;
                println!("literal added: {value} -> #{id}");
            }
            LiteralCmd::List { dir } => {
                let store = open_store(&dir)?;
                let lits = store.literals();
                if lits.is_empty() {
                    println!("no literals found");
                }
                for (id, value) in lits {
                    println!("#{id}\t{value}");
                }
            }
        },

        Cmd::Fact { cmd } => match cmd {
            FactCmd::Add(args) => {
                let mut store = open_store(&args.dir)?;
                let subject_id = resolve_entity(&store, args.subject_id, args.subject)?;
                let predicate_id = resolve_predicate(&store, args.predicate_id, args.predicate)?;
                let object_id = resolve_object(&store, args.object_id, args.object)?;

                let fact = Fact {
                    subject: subject_id,
                    predicate: predicate_id,
                    object: object_id,
                    source: args.source_id,
                    timestamp: Some(args.timestamp.unwrap_or_else(now_ts)),
                    confidence: args.confidence.or(Some(1.0)),
                };
                store.add_fact(fact.clone())?;
                print_fact(&store, &fact, args.pretty);
                println!("fact appended for subject #{subject_id}");
            }
            FactCmd::List(args) => {
                let store = open_store(&args.dir)?;
                handle_fact_list(&store, args)?;
            }
            FactCmd::Query(args) => {
                let store = open_store(&args.dir)?;
                handle_query(&store, args)?;
            }
        },

        Cmd::Query(args) => {
            let store = open_store(&args.dir)?;
            handle_query(&store, args)?;
        }
    }
    Ok(())
}

use anyhow::Result;
use eframe::egui::{self, RichText};
use pru_core::{Fact, PruStore, Query};
use std::path::PathBuf;

const FACT_LIMIT: usize = 500;

#[derive(Default)]
pub struct PruGuiApp {
    pub dir_input: String,
    pub store: Option<PruStore>,
    pub error: Option<String>,
    pub entities: Vec<(u64, String)>,
    pub predicates: Vec<(u64, String)>,
    pub literals: Vec<(u64, String)>,
    pub facts: Vec<Fact>,
    pub selected_entity: Option<u64>,
    pub selected_predicate: Option<u64>,
    pub query_subject: String,
    pub query_predicate: String,
    pub query_object: String,
    pub query_min_confidence: f32,
}

impl PruGuiApp {
    pub fn load_store(&mut self) {
        self.error = None;
        let dir = PathBuf::from(self.dir_input.trim());
        match PruStore::open(&dir) {
            Ok(store) => {
                self.entities = store.entities();
                self.predicates = store.predicates();
                self.literals = store.literals();
                self.selected_entity = self.entities.first().map(|(id, _)| *id);
                self.selected_predicate = None;
                self.facts.clear();
                self.store = Some(store);
                if self.selected_entity.is_some() {
                    if let Err(e) = self.refresh_facts() {
                        self.error = Some(format!("Failed to load facts: {e}"));
                    }
                }
            }
            Err(e) => {
                self.store = None;
                self.error = Some(format!("Failed to open store: {e}"));
            }
        }
    }

    pub fn refresh_facts(&mut self) -> Result<()> {
        let store = match self.store.as_ref() {
            Some(s) => s,
            None => return Ok(()),
        };
        let Some(subject) = self.selected_entity else {
            self.facts.clear();
            return Ok(());
        };

        let mut facts = if let Some(pred) = self.selected_predicate {
            store.facts_for_subject_predicate(subject, pred)?
        } else {
            store.facts_for_subject(subject)?
        };
        facts.truncate(FACT_LIMIT);
        self.facts = facts;
        Ok(())
    }

    fn resolve_entity(&self, name: &str) -> Option<u64> {
        self.store.as_ref().and_then(|s| s.get_entity_id(name))
    }

    fn resolve_predicate(&self, name: &str) -> Option<u64> {
        self.store.as_ref().and_then(|s| s.get_predicate_id(name))
    }

    fn resolve_object(&self, name: &str) -> Option<u64> {
        self.store
            .as_ref()
            .and_then(|s| s.get_literal_id(name).or_else(|| s.get_entity_id(name)))
    }

    fn fact_label(store: &PruStore, fact: &Fact) -> String {
        let s = store
            .get_entity_name(fact.subject)
            .unwrap_or_else(|| format!("#{}", fact.subject));
        let p = store
            .get_predicate_name(fact.predicate)
            .unwrap_or_else(|| format!("#{}", fact.predicate));
        let o = store
            .get_entity_name(fact.object)
            .or_else(|| store.get_literal_value(fact.object))
            .unwrap_or_else(|| format!("#{}", fact.object));
        let conf = fact
            .confidence
            .map(|c| format!(" · conf={:.2}", c))
            .unwrap_or_default();
        let ts = fact
            .timestamp
            .map(|t| format!(" · t={t}"))
            .unwrap_or_default();
        format!("{s} {p} {o}{conf}{ts}")
    }

    fn render_atoms(&mut self, ui: &mut egui::Ui) {
        ui.heading("Atoms");
        ui.separator();
        ui.label(RichText::new("Entities").strong());
        for (id, name) in self.entities.clone() {
            let selected = Some(id) == self.selected_entity;
            if ui
                .selectable_label(selected, format!("{name} (#{id})"))
                .clicked()
            {
                self.selected_entity = Some(id);
                if let Err(e) = self.refresh_facts() {
                    self.error = Some(format!("Failed to refresh facts: {e}"));
                }
            }
        }
        ui.separator();
        ui.label(RichText::new("Predicates").strong());
        for (id, name) in self.predicates.clone() {
            let selected = Some(id) == self.selected_predicate;
            if ui
                .selectable_label(selected, format!("{name} (#{id})"))
                .clicked()
            {
                self.selected_predicate = Some(id);
                if let Err(e) = self.refresh_facts() {
                    self.error = Some(format!("Failed to refresh facts: {e}"));
                }
            }
        }
        ui.separator();
        ui.label(RichText::new("Literals").strong());
        for (id, val) in &self.literals {
            ui.label(format!("{val} (#{id})"));
        }
    }

    fn render_facts(&mut self, ui: &mut egui::Ui) {
        ui.heading("Facts");
        ui.separator();
        if self.store.is_none() {
            ui.label("Open a store to browse facts.");
            return;
        }
        if let Some(subj) = self.selected_entity {
            ui.label(format!("Subject: #{subj}"));
        }
        if self.facts.is_empty() {
            ui.label("No facts for the current filters.");
            return;
        }
        if let Some(store) = self.store.as_ref() {
            egui::ScrollArea::vertical().show(ui, |ui| {
                for fact in &self.facts {
                    ui.horizontal(|ui| {
                        ui.label(Self::fact_label(store, fact));
                        ui.small(format!(
                            "ids: s={} p={} o={}",
                            fact.subject, fact.predicate, fact.object
                        ));
                    });
                    ui.separator();
                }
            });
        }
    }

    fn render_query(&mut self, ui: &mut egui::Ui) {
        ui.heading("Query");
        ui.separator();
        ui.horizontal(|ui| {
            ui.label("Subject name or id");
            ui.text_edit_singleline(&mut self.query_subject);
        });
        ui.horizontal(|ui| {
            ui.label("Predicate name or id");
            ui.text_edit_singleline(&mut self.query_predicate);
        });
        ui.horizontal(|ui| {
            ui.label("Object name or id");
            ui.text_edit_singleline(&mut self.query_object);
        });
        ui.horizontal(|ui| {
            ui.label("Min confidence");
            ui.add(egui::Slider::new(&mut self.query_min_confidence, 0.0..=1.0));
        });
        if ui.button("Run query").clicked() {
            self.run_query();
        }
    }

    fn parse_id(input: &str) -> Option<u64> {
        input.trim().parse::<u64>().ok()
    }

    fn run_query(&mut self) {
        let store = match self.store.as_ref() {
            Some(s) => s,
            None => {
                self.error = Some("Open a store first".to_string());
                return;
            }
        };

        let subject = if self.query_subject.trim().is_empty() {
            None
        } else {
            Self::parse_id(&self.query_subject).or_else(|| self.resolve_entity(&self.query_subject))
        };
        let predicate = if self.query_predicate.trim().is_empty() {
            None
        } else {
            Self::parse_id(&self.query_predicate)
                .or_else(|| self.resolve_predicate(&self.query_predicate))
        };
        let object = if self.query_object.trim().is_empty() {
            None
        } else {
            Self::parse_id(&self.query_object).or_else(|| self.resolve_object(&self.query_object))
        };

        let query = Query {
            subject,
            predicate,
            object,
            min_confidence: Some(self.query_min_confidence),
        };
        match store.query(query) {
            Ok(mut facts) => {
                facts.truncate(FACT_LIMIT);
                self.facts = facts;
                self.selected_entity = subject;
                self.selected_predicate = predicate;
                self.error = None;
            }
            Err(e) => {
                self.error = Some(format!("Query failed: {e}"));
            }
        }
    }
}

impl eframe::App for PruGuiApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("controls").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label("Directory:");
                ui.text_edit_singleline(&mut self.dir_input);
                if ui.button("Open").clicked() {
                    self.load_store();
                }
                if ui.button("Refresh").clicked() {
                    if self.store.is_some() {
                        self.load_store();
                    }
                }
                if let Some(err) = &self.error {
                    ui.colored_label(egui::Color32::from_rgb(200, 60, 60), err);
                }
            });
            if let Some(store) = self.store.as_ref() {
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Overview").strong());
                    ui.label(format!(
                        "entities={} predicates={} literals={} facts={}",
                        store.entities().len(),
                        store.predicates().len(),
                        store.literals().len(),
                        store.fact_count()
                    ));
                });
            } else {
                ui.label("Select a PRU-DB directory to begin.");
            }
        });

        egui::SidePanel::left("atoms").show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                self.render_atoms(ui);
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::CollapsingHeader::new("Facts")
                .default_open(true)
                .show(ui, |ui| {
                    self.render_facts(ui);
                });

            egui::CollapsingHeader::new("Query")
                .default_open(true)
                .show(ui, |ui| {
                    self.render_query(ui);
                });
        });
    }
}

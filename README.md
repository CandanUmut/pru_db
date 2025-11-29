# PRU-DB · Truth Sentinel – Explainable AI vs Human Media Engine

> **Short:** An append-only truth graph (PRU-DB) + a self-improving engine that estimates  
> how likely a piece of content (text, image, audio, video) is **AI-generated** or **human-made**,  
> with explanations instead of absolute claims.

> **Kısa:** PRU-DB üzerinde çalışan, metin/görsel/ses/video içeriklerin **yapay zeka ürünü mü, insan ürünü mü** olduğunu  
> olasılıksal ve **açıklanabilir** şekilde tahmin eden, zamanla kendini geliştiren bir doğruluk motoru.

---

## 1. What is this?

This repository contains:

- **PRU-DB (Precomputed Relational Universe Database)** –  
  an append-only, relational fact store.
- **Truth Sentinel** – a CLI + HTTP service that:
  - ingests media (text, images, later audio/video),
  - runs pluggable detectors,
  - stores all evidence in PRU-DB,
  - computes **AI vs Human probabilities** with explanations,
  - learns from human labels by tracking detector reliability.

This is **not** a magical “100% AI detector”.  
It is a **decision support layer**:

> “Based on all the signals and stored history,  
> this content is, for example, **85% likely AI-generated**, and here is why.”

---

## 2. Why?

Modern models can generate:

- Text (LLMs),
- Images (diffusion models),
- Audio/voice (voice cloning),
- Video (generative video).

Humans, journalists, platforms, and researchers need **help** answering:

- *“Can I trust this image?”*  
- *“Is this article heavily AI-written?”*  
- *“Is this voice clip real or synthesized?”*

Instead of a single black-box classifier, this project aims to be:

- **Explainable** – shows which detectors contributed, with what scores.
- **Self-improving** – human labels update detector reliability over time.
- **Media-agnostic** – same truth engine works for text, images, audio, video.
- **Append-only** – PRU-DB never overwrites facts, it only adds new evidence.

---

## 3. How it works (High-level)

Pipeline:

1. **Ingest**
   - You send text or an image (audio/video later).
   - The system computes a content hash and creates a `Media` entity in PRU-DB.
   - It stores:
     - `has_hash`, `content_type`, etc.

2. **Detection**
   - A `DetectorRegistry` runs all relevant detectors:
     - Example: `TextComplexityDetector`, `ImageMetadataDetector`.
   - For each detector:
     - A score in `[0,1]` (probability of AI),
     - A label (`"ai"` / `"human"` / `"unknown"`),
     - Facts are stored:
       - `detector_score(media, score)` with `source = detector`
       - `detector_label(media, label)` with `source = detector`
       - `analyzed_by(media, detector)`

3. **Truth Engine**
   - Reads:
     - Detector scores and labels,
     - Any human verdicts,
     - Detector reliability stats.
   - If there is a **human verdict**, it dominates:
     - `"ai"` → ~0.99 AI probability  
     - anything else → ~0.01 AI probability
   - Otherwise:
     - It computes a **weighted average** of detector scores:
       - Weight = `default_weight * (correct + 1) / (seen + 2)`
       - Detectors that agree more often with human labels get higher weight.
   - Returns:
     - `probability_ai`
     - `probability_human`
     - `explanations: Vec<String>`

4. **Learning**
   - When you call `/label` or `label` CLI:
     - A `human_verdict(media, label)` fact is stored.
     - Each detector that scored this media updates its reliability:
       - `seen += 1`
       - `correct += 1` if detector’s label matches the human label.
   - Future evaluations automatically use updated reliability.

---

## 4. Project layout

Key components (simplified):

```text
crates/
  pru_core/         # Core PRU-DB store and types
  pru_media_schema/ # Media entities, predicates, helpers, reliability utils
  pru_detectors_api/# Detector traits, registry, basic detectors
  pru_ingest/       # Ingest pipelines for text/image (audio/video later)
  pru_truth_engine/ # AI vs Human probability computation

apps/
  truth_sentinel/   # CLI + HTTP API using all the above


⸻

5. Quick Start

5.1. Build & test

git clone https://github.com/CandanUmut/pru_db.git
cd pru_db

cargo build
cargo test

By default, truth_sentinel uses data/truth_sentinel as its data directory.

⸻

5.2. CLI usage

Run the CLI:

cargo run -p truth_sentinel -- --help

Analyze a text

# Simple example (English):
cargo run -p truth_sentinel -- analyze-text --text "This is a short test text."

# Or read from a file:
cargo run -p truth_sentinel -- analyze-text --file examples/sample.txt

# Or pipe from stdin:
echo "Some content here..." | cargo run -p truth_sentinel -- analyze-text

You’ll get JSON output:

{
  "media_id": 42,
  "probability_ai": 0.73,
  "probability_human": 0.27,
  "explanations": [
    "Detector 7: score_ai=0.78, label=ai"
  ]
}

Analyze an image

cargo run -p truth_sentinel -- analyze-image path/to/image.png

Add a human label

# Label by numeric media id:
cargo run -p truth_sentinel -- label --media 42 --label human

# Or by entity name if you know it:
cargo run -p truth_sentinel -- label --media "media:img:sha256:abcd..." --label ai

This:
	•	adds a human_verdict fact,
	•	updates detector reliability with bump_reliability_from_verdict.

⸻

5.3. HTTP API

Start the server:

cargo run -p truth_sentinel -- serve --addr 127.0.0.1:8080

POST /analyze/text

curl -X POST http://127.0.0.1:8080/analyze/text \
  -H "Content-Type: application/json" \
  -d '{"text": "This is some sample content."}'

Response:

{
  "media_id": 42,
  "probability_ai": 0.61,
  "probability_human": 0.39,
  "explanations": [
    "Detector 7: score_ai=0.61, label=ai"
  ]
}

POST /analyze/image
Raw bytes body (simplest with curl):

curl -X POST http://127.0.0.1:8080/analyze/image \
  --data-binary @path/to/image.png

POST /label

curl -X POST http://127.0.0.1:8080/label \
  -H "Content-Type: application/json" \
  -d '{"media_id": "42", "label": "human"}'

GET /media/:id/report

curl http://127.0.0.1:8080/media/42/report

Returns the same structure as CLI (media id + probabilities + explanations).

⸻

6. Extending the system

The project is designed to be model-agnostic:
	•	You can add new detectors by implementing the MediaDetector trait in pru_detectors_api.
	•	Detectors can:
	•	be pure Rust,
	•	call Python scripts,
	•	query local/remote ML services via HTTP,
	•	or use any external tool.

The truth engine and reliability logic remain the same:
	•	every detector emits scores + labels,
	•	PRU-DB stores them with provenance,
	•	human labels update reliability,
	•	the engine combines everything.

This is ideal for building:
	•	newsroom tools,
	•	moderation backends,
	•	academic research dashboards,
	•	or developer APIs where content trust matters.

⸻

7. Limitations & philosophy
	•	This project does not claim perfect or legal-proof classification.
	•	All outputs are probabilistic and intended as decision support.
	•	The system is designed to be:
	•	transparent,
	•	explainable,
	•	improvable over time with more data and better detectors.

⸻

Türkçe Açıklama · Özet

Bu proje ne?

Bu repo:
	•	PRU-DB: Sadece ekleme yapılan (append-only), ilişkisel bir bilgi/veri tabanı.
	•	Truth Sentinel:
	•	Metin / görsel (ileride ses / video) alan,
	•	Detektörleri çalıştıran,
	•	Sonuçları PRU-DB’ye “gerçek” olarak yazan,
	•	Sonra bu gerçeklerden yola çıkarak
	•	“AI olma ihtimali”,
	•	“insan olma ihtimali”
üreten ve nedenlerini açıklayan bir servis.

Amaç:

“Bu içerik kesin yapay zekâdır” demek değil,
“Elimdeki kanıtlar ve geçmiş veriye göre bu içerik yaklaşık %X ihtimalle AI ürünüdür” demek
ve bunu nedenleriyle birlikte göstermek.

⸻

Nasıl çalışıyor?
	1.	İçeriğin alınması (ingest)
	•	Metni veya görseli gönderiyorsun.
	•	İçerikten hash üretiliyor ve PRU-DB’de bir Media entity’si oluşturuluyor.
	•	Türü (Text, Image vs.) ve hash’i fact olarak ekleniyor.
	2.	Detektörler
	•	DetectorRegistry o medya türü için kayıtlı detektörleri çalıştırıyor.
	•	Her detektör:
	•	0..1 arasında AI skoru,
	•	ai / human / unknown etiketi üretiyor.
	•	Bu skor ve etiketler PRU-DB’de:
	•	detector_score,
	•	detector_label,
	•	analyzed_by şeklinde saklanıyor.
	3.	Truth Engine (hakikat motoru)
	•	Önce human_verdict var mı bakıyor:
	•	Varsa en son verilen etiketi baz alıyor (ör: "ai" → %99).
	•	Yoksa:
	•	Bütün detektör skorlarını topluyor.
	•	Her detektör için PRU-DB’deki güvenilirlik (reliability) bilgisine bakıyor:
	•	seen (kaç örnek gördü),
	•	correct (kaç tanesinde insanla aynı kararı verdi).
	•	Buna göre ağırlık hesaplayıp skorları birleştiriyor:
	•	Daha güvenilir detektör, kararda daha çok ağırlık alıyor.
	•	Sonuç:
	•	probability_ai,
	•	probability_human,
	•	explanations (hangi detektör ne karar verdi).
	4.	Öğrenme
	•	/label veya CLI label ile sen bir medya için:
	•	ai veya human etiketi verdiğinde:
	•	PRU-DB’ye human_verdict fact’i ekleniyor.
	•	O medya için çalışan tüm detektörlerin istatistikleri güncelleniyor:
	•	seen += 1,
	•	Etiket insanla aynıysa correct += 1.
	•	Böylece sistem zamanla hangi detektörün daha güvenilir olduğunu öğreniyor.

⸻

Nasıl denerim?

Kısa versiyon (detaylar yukarıda):

# Build & test
cargo build
cargo test

# CLI'den metin analizi
cargo run -p truth_sentinel -- analyze-text --text "Bu bir deneme metnidir."

# CLI'den görsel analizi
cargo run -p truth_sentinel -- analyze-image path/to/image.png

# Etiket verme
cargo run -p truth_sentinel -- label --media 42 --label human

# HTTP servis
cargo run -p truth_sentinel -- serve --addr 127.0.0.1:8080

Sonra:
	•	POST /analyze/text
	•	POST /analyze/image
	•	POST /label
	•	GET /media/:id/report

endpoint’lerini curl veya herhangi bir HTTP client ile çağırabilirsin.

⸻

Nereye gidebilir?

Bu altyapı üzerine:
	•	Haber siteleri için “yayın öncesi içerik güven skorlaması”,
	•	Okullar / üniversiteler için “AI destekli ödev tespiti” aracı,
	•	Sosyal medya moderasyon araçları,
	•	Marka / telif ihlali tespit sistemleri
	•	Ve genel amaçlı bir “Content Trust API” kurulabilir.

Ama her zaman akılda tutulması gereken:
	•	Bu sistem destek aracıdır, hukuki kesin karar vermez.
	•	Çıktılar olasılıksaldır ve her zaman insanın nihai değerlendirmesiyle birlikte kullanılmalıdır.

⸻



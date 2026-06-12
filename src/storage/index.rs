use crate::error::{LogHavenError, Result};
use crate::storage::record::LogRecord;
use std::path::Path;
use std::sync::Mutex;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{
    IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::{Index, IndexWriter, TantivyDocument};

fn text_stored() -> TextOptions {
    TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    )
}

fn string_stored() -> TextOptions {
    TextOptions::default().set_stored().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::Basic),
    )
}

pub struct AppIndex {
    index: Index,
    writer: Mutex<IndexWriter>,
    pub f_id: tantivy::schema::Field,
    pub f_app: tantivy::schema::Field,
    pub f_timestamp_ms: tantivy::schema::Field,
    pub f_level: tantivy::schema::Field,
    pub f_source: tantivy::schema::Field,
    pub f_message: tantivy::schema::Field,
    pub f_trace_id: tantivy::schema::Field,
    pub f_span_id: tantivy::schema::Field,
    pub f_chain: tantivy::schema::Field,
    pub f_tx_hash: tantivy::schema::Field,
    pub f_tags: tantivy::schema::Field,
    pub f_metadata: tantivy::schema::Field,
}

impl AppIndex {
    pub fn open_or_create(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;

        let mut builder = Schema::builder();

        let f_id = builder.add_text_field("id", string_stored());
        let f_app = builder.add_text_field("app", string_stored());
        let f_timestamp_ms = builder.add_i64_field(
            "timestamp_ms",
            NumericOptions::default()
                .set_stored()
                .set_fast()
                .set_indexed(),
        );
        let f_level = builder.add_text_field("level", string_stored());
        let f_source = builder.add_text_field("source", string_stored());
        let f_message = builder.add_text_field("message", text_stored());
        let f_trace_id = builder.add_text_field("trace_id", string_stored());
        let f_span_id = builder.add_text_field("span_id", string_stored());
        let f_chain = builder.add_text_field("chain", string_stored());
        let f_tx_hash = builder.add_text_field("tx_hash", string_stored());
        let f_tags = builder.add_text_field("tags", text_stored());
        let f_metadata = builder.add_text_field("metadata", text_stored());

        let schema = builder.build();

        let dir = MmapDirectory::open(path).map_err(|e| LogHavenError::Storage(e.to_string()))?;
        let index = Index::open_or_create(dir, schema)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;

        let writer = index
            .writer(50 * 1024 * 1024)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;

        Ok(Self {
            index,
            writer: Mutex::new(writer),
            f_id,
            f_app,
            f_timestamp_ms,
            f_level,
            f_source,
            f_message,
            f_trace_id,
            f_span_id,
            f_chain,
            f_tx_hash,
            f_tags,
            f_metadata,
        })
    }

    pub fn add_record(&self, record: &LogRecord) -> Result<()> {
        let mut doc = TantivyDocument::default();
        doc.add_text(self.f_id, &record.id);
        doc.add_text(self.f_app, &record.app);
        doc.add_i64(self.f_timestamp_ms, record.timestamp_ms);
        doc.add_text(self.f_level, &record.level);
        doc.add_text(self.f_source, &record.source);
        doc.add_text(self.f_message, &record.message);
        if let Some(v) = &record.trace_id {
            doc.add_text(self.f_trace_id, v);
        }
        if let Some(v) = &record.span_id {
            doc.add_text(self.f_span_id, v);
        }
        if let Some(v) = &record.chain {
            doc.add_text(self.f_chain, v);
        }
        if let Some(v) = &record.tx_hash {
            doc.add_text(self.f_tx_hash, v);
        }
        if let Some(v) = &record.tags {
            doc.add_text(self.f_tags, v);
        }
        if let Some(v) = &record.metadata {
            doc.add_text(self.f_metadata, v);
        }

        self.writer
            .lock()
            .unwrap()
            .add_document(doc)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;

        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.writer
            .lock()
            .unwrap()
            .commit()
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<LogRecord>> {
        let reader = self
            .index
            .reader()
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;
        let searcher = reader.searcher();

        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.f_message, self.f_tags, self.f_metadata],
        );
        let query = query_parser
            .parse_query(query_str)
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .map_err(|e| LogHavenError::Storage(e.to_string()))?;

        let mut records = Vec::with_capacity(top_docs.len());
        for (_score, addr) in top_docs {
            let doc: TantivyDocument = searcher
                .doc(addr)
                .map_err(|e| LogHavenError::Storage(e.to_string()))?;

            let get_str = |f: tantivy::schema::Field| -> String {
                doc.get_first(f)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            };
            let get_opt = |f: tantivy::schema::Field| -> Option<String> {
                doc.get_first(f)
                    .and_then(|v| v.as_str())
                    .filter(|s: &&str| !s.is_empty())
                    .map(|s| s.to_string())
            };
            let get_i64 = |f: tantivy::schema::Field| -> i64 {
                doc.get_first(f).and_then(|v| v.as_i64()).unwrap_or(0)
            };

            records.push(LogRecord {
                id: get_str(self.f_id),
                app: get_str(self.f_app),
                timestamp_ms: get_i64(self.f_timestamp_ms),
                level: get_str(self.f_level),
                source: get_str(self.f_source),
                message: get_str(self.f_message),
                trace_id: get_opt(self.f_trace_id),
                span_id: get_opt(self.f_span_id),
                chain: get_opt(self.f_chain),
                tx_hash: get_opt(self.f_tx_hash),
                tags: get_opt(self.f_tags),
                metadata: get_opt(self.f_metadata),
            });
        }

        Ok(records)
    }
}

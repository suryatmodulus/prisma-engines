use std::collections::HashMap;

use sql_schema_describer::SqlSchema;
use crate::pair::Pair;

pub(crate) struct DifferDatabase<'a> {
    schemas: Pair<&'a SqlSchema>,
    table_names: HashMap<&'a str, Pair<Option<usize>>>,
    created_tables: Vec<usize>,
    table_pairs: Vec<Pair<usize>>,
    dropped_tables: Vec<usize>,
}

impl<'a> DifferDatabase<'a> {
    pub(crate) fn new(schemas: Pair<&'a SqlSchema>) -> Self {
        let table_names_count_lb = std::cmp::max(
            schemas.previous().tables.len(), schemas.next().tables.len()
        );
        let mut table_names = HashMap::with_capacity(table_names_count_lb);

        // We are biased to created tables in the first pass, because migrations
        // tend to add rather than remove.
        let mut created_tables = Vec::with_capacity(schemas.next().tables.len().saturating_sub(schemas.previous().tables.len()));
        let mut table_pairs = Vec::new();

        for table in schemas.previous().table_walkers() {
            table_names.insert(table.name(), Pair::new(Some(table.table_index()), None ));
        }

        for table in schemas.next().table_walkers() {
            let entry = table_names.entry(table.name()).or_default();
            *entry.next_mut() = Some(table.table_index());

            match entry.previous() {
                Some(previous_idx) => {
                    table_pairs.push(Pair::new(*previous_idx,table.table_index()));
                }
                None => {
                    created_tables.push(table.table_index())
                }
            }
        }

        let dropped_tables = todo!("A clever way to computed dropped tables without walking the tables again");


        DifferDatabase { schemas, table_names, created_tables, table_pairs, dropped_tables }
    }

    pub(crate) fn created_tables(&self) -> &[usize] {
        &self.created_tables
    }
}
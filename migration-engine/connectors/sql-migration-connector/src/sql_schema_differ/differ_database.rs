use crate::{flavour::SqlFlavour, pair::Pair};
use sql_schema_describer::{
    walkers::{SqlSchemaExt, TableWalker},
    SqlSchema,
};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, PartialOrd, Ord, Eq, Hash)]
struct TableName<'a> {
    case_insensitive: bool,
    name: &'a str,
}

impl TableName<'_> {
    fn new_case_sensitive(name: &str) -> TableName<'_> {
        TableName {
            case_insensitive: false,
            name,
        }
    }

    fn new_case_insensitive(name: &str) -> TableName<'_> {
        TableName {
            case_insensitive: false,
            name,
        }
    }
}

impl PartialEq for TableName<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.case_insensitive {
            self.name.eq_ignore_ascii_case(other.name)
        } else {
            self.name == other.name
        }
    }
}

pub(crate) struct DifferDatabase<'a> {
    schemas: Pair<&'a SqlSchema>,
    table_names: HashMap<TableName<'a>, Pair<Option<usize>>>,
    created_tables: Vec<usize>,
    table_pairs: Vec<Pair<usize>>,
    dropped_tables: Vec<usize>,
    column_names: BTreeMap<(TableName<'a>, &'a str), Pair<Option<usize>>>,
    tables_to_redefine: HashSet<Pair<usize>>,
}

impl<'a> DifferDatabase<'a> {
    pub(crate) fn new(schemas: Pair<&'a SqlSchema>, flavour: &'a dyn SqlFlavour) -> Self {
        let table_names_count_lb = std::cmp::max(schemas.previous().tables.len(), schemas.next().tables.len());

        let new_table_name: &dyn Fn(&str) -> TableName<'_> = if flavour.lower_cases_table_names() {
            &TableName::new_case_insensitive
        } else {
            &TableName::new_case_sensitive
        };

        let mut table_names = HashMap::with_capacity(table_names_count_lb);
        let mut column_names = BTreeMap::new();

        // We are biased to created tables in the first pass, because migrations
        // tend to add rather than remove.
        let mut created_tables = Vec::with_capacity(
            schemas
                .next()
                .tables
                .len()
                .saturating_sub(schemas.previous().tables.len()),
        );
        let mut table_pairs = Vec::new();

        for table in schemas.previous().table_walkers() {
            let table_name = new_table_name(table.name());
            table_names.insert(table_name, Pair::new(Some(table.table_index()), None));

            for column in table.columns() {
                column_names.insert(
                    (table_name, column.name()),
                    Pair::new(Some(column.column_index()), None),
                );
            }
        }

        for table in schemas.next().table_walkers() {
            let table_name = new_table_name(table.name());
            let entry = table_names.entry(table_name).or_default();
            *entry.next_mut() = Some(table.table_index());

            match entry.previous() {
                Some(previous_idx) => {
                    table_pairs.push(Pair::new(*previous_idx, table.table_index()));
                }
                None => created_tables.push(table.table_index()),
            }

            for column in table.columns() {
                let entry = column_names.entry((table_name, column.name())).or_default();
                *entry.next_mut() = Some(column.column_index());
            }
        }

        let dropped_tables = table_names
            .values()
            .filter(|t| t.next().is_none())
            .filter_map(|t| *t.previous())
            .collect();

        let mut db = DifferDatabase {
            schemas,
            table_names,
            created_tables,
            table_pairs,
            dropped_tables,
            column_names,
            tables_to_redefine: HashSet::new(),
        };

        let tables_to_redefine = flavour.tables_to_redefine(db);
        db.tables_to_redefine = tables_to_redefine;

        db
    }

    pub(crate) fn created_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.created_tables
            .iter()
            .map(move |idx| self.schemas.next().table_walker_at(*idx))
    }

    pub(crate) fn dropped_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.dropped_tables
            .iter()
            .map(move |idx| self.schemas.next().table_walker_at(*idx))
    }

    pub(crate) fn dropped_tables_count(&self) -> usize {
        self.dropped_tables.len()
    }

    pub(crate) fn table_pairs(&self) -> impl Iterator<Item = Pair<TableWalker<'a>>> + '_ {
        self.table_pairs.iter().map(move |idxs| self.schemas.tables(idxs))
    }
}

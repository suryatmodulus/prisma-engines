use crate::{flavour::SqlFlavour, pair::Pair};
use sql_schema_describer::{
    walkers::{SqlSchemaExt, TableWalker},
    SqlSchema,
};
use std::{borrow::Cow, collections::HashMap};

use super::table::TableDiffer;

/// The responsibility of this component is to guarantee that walking two
/// schemas during diffing does not incur unnecessary computational complexity.
pub(crate) struct DifferDatabase<'a> {
    schemas: Pair<&'a SqlSchema>,
    flavour: &'a dyn SqlFlavour,
    tables: HashMap<Cow<'a, str>, Pair<Option<usize>>>,
    columns: HashMap<(&'a str, &'a str), Pair<Option<(usize, usize)>>>,
}

impl<'a> DifferDatabase<'a> {
    /// Initializes the database. This will walk over both schemas once. Complexity: O(n) over the number of columns in the schemas.
    pub(crate) fn new(schemas: Pair<&'a SqlSchema>, flavour: &'a dyn SqlFlavour) -> Self {
        let tables_count_lower_bound = std::cmp::max(schemas.previous().tables.len(), schemas.next().tables.len());

        let mut tables = HashMap::with_capacity(tables_count_lower_bound);
        let mut columns = HashMap::with_capacity(tables_count_lower_bound * 2);

        for table in schemas.previous().table_walkers() {
            tables.insert(
                flavour.normalize_table_name(table.name()),
                Pair::new(Some(table.table_index()), None),
            );

            for column in table.columns() {
                columns.insert(
                    (table.name(), column.name()),
                    Pair::new(Some((table.table_index(), column.column_index())), None),
                );
            }
        }

        for table in schemas.next().table_walkers() {
            let entry = tables.entry(flavour.normalize_table_name(table.name())).or_default();
            *entry.next_mut() = Some(table.table_index());

            for column in table.columns() {
                let entry = columns.entry((table.name(), column.name())).or_default();
                *entry.next_mut() = Some((table.table_index(), column.column_index()));
            }
        }

        DifferDatabase {
            schemas,
            flavour,
            tables,
            columns,
        }
    }

    /// Complexity is O(n) over the number of distinct tables in the two schemas.
    pub(crate) fn created_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.tables
            .values()
            .filter(|t| t.previous().is_none())
            .filter_map(|t| *t.next())
            .map(move |table_idx| self.schemas.next().table_walker_at(table_idx))
    }

    /// Complexity is O(n) over the number of distinct tables in the two schemas.
    pub(crate) fn dropped_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.tables
            .values()
            .filter(|t| t.previous().is_none())
            .filter_map(|t| *t.next())
            .map(move |table_idx| self.schemas.next().table_walker_at(table_idx))
    }

    pub(crate) fn flavour(&self) -> &'a dyn SqlFlavour {
        self.flavour
    }

    fn enum_pairs(&self) -> impl Iterator<Item = EnumDiffer<'_>> {
        self.previous_enums().filter_map(move |previous| {
            self.next_enums()
                .find(|next| enums_match(&previous, &next))
                .map(|next| EnumDiffer {
                    enums: Pair::new(previous, next),
                })
        })
    }

    fn created_enums<'a>(&'a self) -> impl Iterator<Item = EnumWalker<'schema>> + 'a {
        self.next_enums()
            .filter(move |next| !self.previous_enums().any(|previous| enums_match(&previous, next)))
    }

    fn dropped_enums<'a>(&'a self) -> impl Iterator<Item = EnumWalker<'schema>> + 'a {
        self.previous_enums()
            .filter(move |previous| !self.next_enums().any(|next| enums_match(previous, &next)))
    }

    fn previous_enums(&self) -> impl Iterator<Item = EnumWalker<'schema>> {
        self.schemas.previous().enum_walkers()
    }

    fn next_enums(&self) -> impl Iterator<Item = EnumWalker<'schema>> {
        self.schemas.next().enum_walkers()
    }

    pub(crate) fn previous_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.schemas
            .previous()
            .table_walkers()
            .filter(move |table| !self.table_is_ignored(&table.name()))
    }

    pub(crate) fn next_tables(&self) -> impl Iterator<Item = TableWalker<'a>> + '_ {
        self.schemas
            .next()
            .table_walkers()
            .filter(move |table| !self.table_is_ignored(&table.name()))
    }

    fn table_is_ignored(&self, table_name: &str) -> bool {
        table_name == "_prisma_migrations" || self.flavour.table_should_be_ignored(&table_name)
    }

    /// An iterator over the tables that are present in both schemas, excluding
    /// tables that will be completely redefined.
    ///
    /// Complexity is O(n) over the number of distinct tables in the two
    /// schemas.
    pub(crate) fn table_pairs(&self) -> impl Iterator<Item = TableDiffer<'a>> + '_ {
        self.tables.values().filter_map(|t| t.transpose()).map(|t| TableDiffer {
            tables: self.schemas.tables(*t),
        })
    }
}

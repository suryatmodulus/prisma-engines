mod apply_migrations;
mod create_migration;
mod dev_diagnostic;
mod diagnose_migration_history;
mod evaluate_data_loss;
mod list_migration_directories;
mod mark_migration_applied;
mod mark_migration_rolled_back;
mod reset;
mod schema_push;

pub use apply_migrations::ApplyMigrations;
pub use create_migration::CreateMigration;
pub use diagnose_migration_history::DiagnoseMigrationHistory;
pub use evaluate_data_loss::EvaluateDataLoss;
pub use mark_migration_applied::MarkMigrationApplied;
pub use reset::Reset;
pub use schema_push::SchemaPush;

use crate::{
    assertions::SchemaAssertion, sql::barrel_migration_executor::BarrelMigrationExecutor,
    test_api::list_migration_directories::ListMigrationDirectories, AssertionResult,
};
use dev_diagnostic::DevDiagnostic;
use mark_migration_rolled_back::MarkMigrationRolledBack;
use migration_connector::{ConnectorError, MigrationPersistence, MigrationRecord};
use quaint::{
    prelude::{ConnectionInfo, Queryable, SqlFamily},
    single::Quaint,
};
use sql_migration_connector::SqlMigrationConnector;
use std::{borrow::Cow, fmt::Write as _};
use tempfile::TempDir;
use test_setup::{create_mysql_database, create_postgres_database, sqlite_test_url, BitFlags, Tags, TestApiArgs};

/// A handle to all the context needed for end-to-end testing of the migration engine across
/// connectors.
pub struct TestApi {
    api: SqlMigrationConnector,
    args: TestApiArgs,
    connection_string: String,
}

impl TestApi {
    pub async fn new(args: TestApiArgs) -> Self {
        let tags = args.tags();

        let db_name = if tags.contains(Tags::Mysql) {
            test_setup::mysql_safe_identifier(args.test_function_name())
        } else {
            args.test_function_name()
        };

        let (_conn, connection_string): (_, String) = if tags.contains(Tags::Mysql) {
            create_mysql_database(&db_name).await.unwrap()
        } else if tags.contains(Tags::Postgres) {
            create_postgres_database(&db_name).await.unwrap()
        } else if tags.contains(Tags::Mssql) {
            test_setup::init_mssql_database(args.database_url(), db_name)
                .await
                .unwrap()
        } else if tags.contains(Tags::Sqlite) {
            let url = sqlite_test_url(db_name);

            (Quaint::new(&url).await.unwrap(), url)
        } else {
            unreachable!()
        };

        let api = SqlMigrationConnector::new(&connection_string, args.shadow_database_url().map(String::from))
            .await
            .unwrap();

        let mut circumstances = BitFlags::empty();

        if tags.contains(Tags::Mysql) {
            let val = api
                .quaint()
                .query_raw("SELECT @@lower_case_table_names", &[])
                .await
                .ok()
                .and_then(|row| row.into_single().ok())
                .and_then(|row| row.at(0).and_then(|col| col.as_i64()))
                .filter(|val| *val == 1);

            if val.is_some() {
                circumstances |= Tags::LowerCasesTableNames;
            }
        }

        TestApi {
            api,
            args,
            connection_string,
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn schema_name(&self) -> &str {
        self.connection_info().schema_name()
    }

    pub fn database(&self) -> &Quaint {
        &self.api.quaint()
    }

    pub fn is_sqlite(&self) -> bool {
        self.tags().contains(Tags::Sqlite)
    }

    pub fn is_mssql(&self) -> bool {
        self.tags().contains(Tags::Mssql)
    }

    pub fn is_mysql(&self) -> bool {
        self.tags().contains(Tags::Mysql)
    }

    pub fn is_mysql_8(&self) -> bool {
        self.tags().contains(Tags::Mysql8)
    }

    pub fn is_mariadb(&self) -> bool {
        self.tags().contains(Tags::Mariadb)
    }

    pub fn lower_case_identifiers(&self) -> bool {
        self.tags().contains(Tags::LowerCasesTableNames)
    }

    pub fn is_mysql_5_6(&self) -> bool {
        self.tags().contains(Tags::Mysql56)
    }

    pub fn is_postgres(&self) -> bool {
        self.tags().contains(Tags::Postgres)
    }

    pub fn migration_persistence<'a>(&'a self) -> &(dyn MigrationPersistence + 'a) {
        &self.api
    }

    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.database().connection_info()
    }

    pub fn sql_family(&self) -> SqlFamily {
        self.connection_info().sql_family()
    }

    pub fn tags(&self) -> BitFlags<Tags> {
        self.args.tags()
    }

    pub fn datasource(&self) -> String {
        self.args.datasource_block(&self.connection_string)
    }

    /// Render a table name with the required prefixing for use with quaint query building.
    pub fn render_table_name<'a>(&'a self, table_name: &'a str) -> quaint::ast::Table<'a> {
        if self.is_sqlite() {
            table_name.into()
        } else {
            (self.connection_info().schema_name(), table_name).into()
        }
    }

    /// Create a temporary directory to serve as a test migrations directory.
    pub fn create_migrations_directory(&self) -> std::io::Result<TempDir> {
        tempfile::tempdir()
    }

    pub fn display_migrations(&self, migrations_directory: &TempDir) -> std::io::Result<()> {
        for entry in std::fs::read_dir(migrations_directory.path())? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for file in std::fs::read_dir(&entry.path())? {
                    let entry = file?;

                    if entry.file_type()?.is_dir() {
                        continue;
                    }

                    let s = std::fs::read_to_string(entry.path())?;
                    tracing::info!(path = ?entry.path(), contents = ?s);
                }
            } else {
                let s = std::fs::read_to_string(entry.path())?;
                tracing::info!(path = ?entry.path(), contents = ?s);
            }
        }

        Ok(())
    }

    pub fn apply_migrations<'a>(&'a self, migrations_directory: &'a TempDir) -> ApplyMigrations<'a> {
        ApplyMigrations::new(&self.api, migrations_directory)
    }

    pub fn list_migration_directories<'a>(&'a self, migrations_directory: &'a TempDir) -> ListMigrationDirectories<'a> {
        ListMigrationDirectories::new(&self.api, migrations_directory)
    }

    /// Convenient builder and assertions for the CreateMigration command.
    pub fn create_migration<'a>(
        &'a self,
        name: &'a str,
        prisma_schema: &'a str,
        migrations_directory: &'a TempDir,
    ) -> CreateMigration<'a> {
        CreateMigration::new(&self.api, name, prisma_schema, migrations_directory)
    }

    /// Builder and assertions to call the `devDiagnostic` command.
    pub fn dev_diagnostic<'a>(&'a self, migrations_directory: &'a TempDir) -> DevDiagnostic<'a> {
        DevDiagnostic::new(&self.api, migrations_directory)
    }

    /// Builder and assertions to call the DiagnoseMigrationHistory command.
    pub fn diagnose_migration_history<'a>(&'a self, migrations_directory: &'a TempDir) -> DiagnoseMigrationHistory<'a> {
        DiagnoseMigrationHistory::new(&self.api, migrations_directory)
    }

    pub fn evaluate_data_loss<'a>(
        &'a self,
        migrations_directory: &'a TempDir,
        prisma_schema: impl Into<String>,
    ) -> EvaluateDataLoss<'a> {
        EvaluateDataLoss::new(&self.api, migrations_directory, prisma_schema.into())
    }

    pub fn mark_migration_applied<'a>(
        &'a self,
        migration_name: impl Into<String>,
        migrations_directory: &'a TempDir,
    ) -> MarkMigrationApplied<'a> {
        MarkMigrationApplied::new(&self.api, migration_name.into(), migrations_directory)
    }

    pub fn mark_migration_rolled_back(&self, migration_name: impl Into<String>) -> MarkMigrationRolledBack<'_> {
        MarkMigrationRolledBack::new(&self.api, migration_name.into())
    }

    pub fn reset(&self) -> Reset<'_> {
        Reset::new(&self.api)
    }

    pub fn schema_push(&self, dm: impl Into<String>) -> SchemaPush<'_> {
        SchemaPush::new(&self.api, dm.into())
    }

    pub fn barrel(&self) -> BarrelMigrationExecutor<'_> {
        BarrelMigrationExecutor {
            api: self,
            sql_variant: match self.sql_family() {
                SqlFamily::Mysql => barrel::SqlVariant::Mysql,
                SqlFamily::Postgres => barrel::SqlVariant::Pg,
                SqlFamily::Sqlite => barrel::SqlVariant::Sqlite,
                SqlFamily::Mssql => barrel::SqlVariant::Mssql,
            },
        }
    }

    pub async fn assert_schema(&self) -> Result<SchemaAssertion, ConnectorError> {
        let schema = self.api.describe_schema().await?;
        Ok(SchemaAssertion::new(schema, self.tags()))
    }

    pub async fn dump_table(&self, table_name: &str) -> Result<quaint::prelude::ResultSet, quaint::error::Error> {
        let select_star =
            quaint::ast::Select::from_table(self.render_table_name(table_name)).value(quaint::ast::asterisk());

        self.database().query(select_star.into()).await
    }

    pub fn insert<'a>(&'a self, table_name: &'a str) -> SingleRowInsert<'a> {
        SingleRowInsert {
            insert: quaint::ast::Insert::single_into(self.render_table_name(table_name)),
            api: self,
        }
    }

    pub fn select<'a>(&'a self, table_name: &'a str) -> TestApiSelect<'_> {
        TestApiSelect {
            select: quaint::ast::Select::from_table(self.render_table_name(table_name)),
            api: self,
        }
    }

    pub fn write_native_types_datamodel_header(&self, buf: &mut String) {
        indoc::writedoc!(
            buf,
            r#"
            datasource test_db {{
                provider = "{provider}"
                url      = "{provider}://localhost:666"
              }}

            "#,
            provider = self.args.provider()
        )
        .unwrap();
    }

    pub fn native_types_datamodel(&self, schema: &str) -> String {
        let mut out = String::with_capacity(320 + schema.len());

        self.write_native_types_datamodel_header(&mut out);
        out.push_str(schema);

        out
    }

    pub fn normalize_identifier<'a>(&self, identifier: &'a str) -> Cow<'a, str> {
        if self.lower_case_identifiers() {
            identifier.to_ascii_lowercase().into()
        } else {
            identifier.into()
        }
    }
}

pub struct SingleRowInsert<'a> {
    insert: quaint::ast::SingleRowInsert<'a>,
    api: &'a TestApi,
}

impl<'a> SingleRowInsert<'a> {
    pub fn value(mut self, name: &'a str, value: impl Into<quaint::ast::Expression<'a>>) -> Self {
        self.insert = self.insert.value(name, value);

        self
    }

    pub async fn result_raw(self) -> Result<quaint::connector::ResultSet, anyhow::Error> {
        Ok(self.api.database().query(self.insert.into()).await?)
    }
}

pub struct TestApiSelect<'a> {
    select: quaint::ast::Select<'a>,
    api: &'a TestApi,
}

impl<'a> TestApiSelect<'a> {
    pub fn column(mut self, name: &'a str) -> Self {
        self.select = self.select.column(name);

        self
    }

    /// This is deprecated. Used row assertions instead with the ResultSetExt trait.
    pub async fn send_debug(self) -> Result<Vec<Vec<String>>, quaint::error::Error> {
        let rows = self.send().await?;

        let rows: Vec<Vec<String>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(|col| format!("{:?}", col)).collect())
            .collect();

        Ok(rows)
    }

    pub async fn send(self) -> Result<quaint::prelude::ResultSet, quaint::error::Error> {
        Ok(self.api.database().query(self.select.into()).await?)
    }
}

pub trait MigrationsAssertions: Sized {
    fn assert_applied_steps_count(self, count: u32) -> AssertionResult<Self>;
    fn assert_checksum(self, expected: &str) -> AssertionResult<Self>;
    fn assert_failed(self) -> AssertionResult<Self>;
    fn assert_logs(self, expected: &str) -> AssertionResult<Self>;
    fn assert_migration_name(self, expected: &str) -> AssertionResult<Self>;
    fn assert_success(self) -> AssertionResult<Self>;
}

impl MigrationsAssertions for MigrationRecord {
    fn assert_checksum(self, expected: &str) -> AssertionResult<Self> {
        assert_eq!(self.checksum, expected);

        Ok(self)
    }

    fn assert_migration_name(self, expected: &str) -> AssertionResult<Self> {
        assert_eq!(&self.migration_name[15..], expected);

        Ok(self)
    }

    fn assert_logs(self, expected: &str) -> AssertionResult<Self> {
        assert_eq!(self.logs.as_deref(), Some(expected));

        Ok(self)
    }

    fn assert_applied_steps_count(self, count: u32) -> AssertionResult<Self> {
        assert_eq!(self.applied_steps_count, count);

        Ok(self)
    }

    fn assert_success(self) -> AssertionResult<Self> {
        assert!(self.finished_at.is_some());

        Ok(self)
    }

    fn assert_failed(self) -> AssertionResult<Self> {
        assert!(self.finished_at.is_none() && self.rolled_back_at.is_none());

        Ok(self)
    }
}

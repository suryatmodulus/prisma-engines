#![deny(missing_docs)]

//! A TestApi that is initialized without IO or async code and can instantiate
//! multiple migration engines.

pub use test_setup::{BitFlags, Capabilities, Tags};

use crate::{ApplyMigrations, CreateMigration, DiagnoseMigrationHistory, Reset, SchemaAssertion, SchemaPush};
use migration_core::GenericApi;
use quaint::{prelude::Queryable, single::Quaint};
use sql_migration_connector::SqlMigrationConnector;
use std::future::Future;
use tempfile::TempDir;
use test_setup::TestApiArgs;

/// The multi-engine test API.
pub struct TestApi {
    args: TestApiArgs,
    connection_string: String,
    admin_conn: Quaint,
    rt: tokio::runtime::Runtime,
}

impl TestApi {
    /// Initializer, called by the test macros.
    pub fn new(args: TestApiArgs) -> Self {
        let rt = test_setup::runtime::test_tokio_runtime();
        let tags = args.tags();
        let db_name = args.test_function_name();

        let (admin_conn, connection_string) = if tags.contains(Tags::Postgres) {
            rt.block_on(test_setup::create_postgres_database(db_name)).unwrap()
        } else if tags.contains(Tags::Mysql) {
            rt.block_on(test_setup::create_mysql_database(db_name)).unwrap()
        } else if tags.contains(Tags::Mssql) {
            rt.block_on(test_setup::init_mssql_database(args.database_url(), db_name))
                .unwrap()
        } else if tags.contains(Tags::Sqlite) {
            let url = test_setup::sqlite_test_url(db_name);

            (rt.block_on(Quaint::new(&url)).unwrap(), url)
        } else {
            unreachable!()
        };

        TestApi {
            args,
            admin_conn,
            connection_string,
            rt,
        }
    }

    /// Block on a future
    pub fn block_on<O, F: Future<Output = O>>(&self, f: F) -> O {
        self.rt.block_on(f)
    }

    /// Equivalent to quaint's query_raw()
    pub fn query_raw(&self, sql: &str, params: &[quaint::Value<'_>]) -> quaint::Result<quaint::prelude::ResultSet> {
        self.block_on(self.admin_conn.query_raw(sql, params))
    }

    /// Send a SQL command to the database, and expect it to succeed.
    pub fn raw_cmd(&self, sql: &str) {
        self.rt.block_on(self.admin_conn.raw_cmd(sql)).unwrap()
    }

    /// The connection string for the database associated with the test.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Create a temporary directory to serve as a test migrations directory.
    pub fn create_migrations_directory(&self) -> TempDir {
        tempfile::tempdir().unwrap()
    }

    /// Returns true only when testing on MSSQL.
    pub fn is_mssql(&self) -> bool {
        self.tags().contains(Tags::Mssql)
    }

    /// Returns true only when testing on MySQL.
    pub fn is_mysql(&self) -> bool {
        self.tags().contains(Tags::Mysql)
    }

    /// Returns true only when testing on MariaDB.
    pub fn is_mysql_mariadb(&self) -> bool {
        self.tags().contains(Tags::Mariadb)
    }

    /// Returns true only when testing on MySQL 5.6.
    pub fn is_mysql_5_6(&self) -> bool {
        self.tags().contains(Tags::Mysql56)
    }

    /// Returns true only when testing on MySQL 8.
    pub fn is_mysql_8(&self) -> bool {
        self.tags().contains(Tags::Mysql8)
    }

    /// Returns true only when testing on postgres.
    pub fn is_postgres(&self) -> bool {
        self.tags().contains(Tags::Postgres)
    }

    /// Returns true only when testing on vitess.
    pub fn is_vitess(&self) -> bool {
        self.tags().contains(Tags::Vitess)
    }

    /// Instantiate a new migration engine for the current database.
    pub fn new_engine(&self) -> EngineTestApi<'_> {
        let shadow_db = self.args.shadow_database_url().as_ref().map(ToString::to_string);

        self.new_engine_with_connection_strings(&self.connection_string, shadow_db)
    }

    /// Instantiate a new migration with the provided connection string.
    pub fn new_engine_with_connection_strings(
        &self,
        connection_string: &str,
        shadow_db_connection_string: Option<String>,
    ) -> EngineTestApi<'_> {
        let connector = self
            .rt
            .block_on(SqlMigrationConnector::new(
                &connection_string,
                shadow_db_connection_string,
            ))
            .unwrap();

        EngineTestApi {
            connector,
            tags: self.args.tags(),
            rt: &self.rt,
        }
    }

    fn tags(&self) -> BitFlags<Tags> {
        self.args.tags()
    }

    /// The name of the test function, as a string.
    pub fn test_fn_name(&self) -> &str {
        self.args.test_function_name()
    }
}

/// A wrapper around a migration engine instance optimized for convenience in
/// writing tests.
pub struct EngineTestApi<'a> {
    connector: SqlMigrationConnector,
    tags: BitFlags<Tags>,
    rt: &'a tokio::runtime::Runtime,
}

impl EngineTestApi<'_> {
    /// Plan an `applyMigrations` command
    pub fn apply_migrations<'a>(&'a self, migrations_directory: &'a TempDir) -> ApplyMigrations<'a> {
        ApplyMigrations::new_sync(&self.connector, migrations_directory, &self.rt)
    }

    /// Plan a `createMigration` command
    pub fn create_migration<'a>(
        &'a self,
        name: &'a str,
        schema: &'a str,
        migrations_directory: &'a TempDir,
    ) -> CreateMigration<'a> {
        CreateMigration::new_sync(&self.connector, name, schema, migrations_directory, &self.rt)
    }

    /// Builder and assertions to call the DiagnoseMigrationHistory command.
    pub fn diagnose_migration_history<'a>(&'a self, migrations_directory: &'a TempDir) -> DiagnoseMigrationHistory<'a> {
        DiagnoseMigrationHistory::new(&self.connector, migrations_directory)
    }

    /// Assert facts about the database schema
    pub fn assert_schema(&self) -> SchemaAssertion {
        SchemaAssertion::new(self.rt.block_on(self.connector.describe_schema()).unwrap(), self.tags)
    }

    /// Expose the GenericApi impl.
    pub fn generic_api(&self) -> &dyn GenericApi {
        &self.connector
    }

    /// Plan a `reset` command
    pub fn reset(&self) -> Reset<'_> {
        Reset::new_sync(&self.connector, &self.rt)
    }

    /// Plan a `schemaPush` command
    pub fn schema_push(&self, dm: impl Into<String>) -> SchemaPush<'_> {
        SchemaPush::new_sync(&self.connector, dm.into(), &self.rt)
    }

    /// The schema name of the current connected database.
    pub fn schema_name(&self) -> &str {
        self.connector.quaint().connection_info().schema_name()
    }

    /// Execute a raw SQL command.
    pub fn raw_cmd(&self, cmd: &str) -> Result<(), quaint::error::Error> {
        self.rt.block_on(self.connector.quaint().raw_cmd(cmd))
    }
}

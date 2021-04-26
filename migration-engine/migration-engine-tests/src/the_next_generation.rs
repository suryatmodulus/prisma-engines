pub use enumflags2::BitFlags;

use crate::SchemaAssertion;
use migration_core::{commands::SchemaPushOutput, GenericApi, SchemaPushInput};
use sql_migration_connector::SqlMigrationConnector;

pub struct TestApi {
    rt: tokio::runtime::Runtime,
    connector: SqlMigrationConnector,
}

impl TestApi {
    pub fn new() -> Self {
        todo!()
    }

    pub fn assert_schema(&self) -> SchemaAssertion {
        let schema = self.rt.block_on(self.connector.describe_schema()).unwrap();

        SchemaAssertion::new(schema, BitFlags::empty())
    }

    pub fn schema_push(&self, dm: impl Into<String>) -> SchemaPush<'_> {
        SchemaPush {
            api: &self.connector,
            dm: dm.into(),
            rt: self.rt.handle(),
        }
    }
}

pub struct SchemaPush<'a> {
    api: &'a dyn GenericApi,
    dm: String,
    rt: &'a tokio::runtime::Handle,
}

impl SchemaPush<'_> {
    pub fn send(self) -> SchemaPushAssertion {
        let output = self
            .rt
            .block_on(self.api.schema_push(&SchemaPushInput {
                schema: self.dm,
                force: true,
                assume_empty: false,
            }))
            .unwrap();

        SchemaPushAssertion { output }
    }
}

pub struct SchemaPushAssertion {
    output: SchemaPushOutput,
}

impl SchemaPushAssertion {
    pub fn assert_green(self) -> Self {
        assert!(self.output.warnings.is_empty());
        assert!(self.output.unexecutable.is_empty());

        self
    }

    pub fn assert_no_steps(self) -> Self {
        assert_eq!(self.output.executed_steps, 0);

        self
    }
}

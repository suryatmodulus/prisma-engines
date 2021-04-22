use enumflags2::BitFlags;
use migration_core::{commands::SchemaPushOutput, GenericApi, SchemaPushInput};
use sql_migration_connector::SqlMigrationConnector;
use test_macros::test_case;
use test_setup::connectors::Tags;

struct TestApi {
    rt: tokio::runtime::Runtime,
    connector: SqlMigrationConnector,
}

impl TestApi {
    fn new() -> Self {
        todo!()
    }

    fn schema_push(&self, dm: impl Into<String>) -> SchemaPush<'_> {
        SchemaPush {
            api: &self.connector,
            dm: dm.into(),
            rt: self.rt.handle(),
        }
    }
}

struct SchemaPush<'a> {
    api: &'a dyn GenericApi,
    dm: String,
    rt: &'a tokio::runtime::Handle,
}

impl SchemaPush<'_> {
    fn send(self) -> SchemaPushAssertion {
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

struct SchemaPushAssertion {
    output: SchemaPushOutput,
}

impl SchemaPushAssertion {
    fn assert_green(self) -> Self {
        assert!(self.output.warnings.is_empty());
        assert!(self.output.unexecutable.is_empty());

        self
    }
}

#[test_case(tags(Sqlite))]
async fn sqlite_must_recreate_indexes(api: TestApi) {
    // SQLite must go through a complicated migration procedure which requires dropping and recreating indexes. This test checks that.
    // We run them still against each connector.
    let dm1 = r#"
        model A {
            id Int @id
            field String @unique
        }
    "#;

    api.schema_push(dm1).send().assert_green();

    api.assert_schema().assert_table("A", |table| {
        table.assert_index_on_columns(&["field"], |idx| idx.assert_is_unique())
    });

    let dm2 = r#"
        model A {
            id    Int    @id
            field String @unique
            other String
        }
    "#;

    api.schema_push(dm2).send().assert_green();

    api.assert_schema().assert_table("A", |table| {
        table.assert_index_on_columns(&["field"], |idx| idx.assert_is_unique())
    });
}

#[test_case(tags(Sqlite))]
async fn sqlite_must_recreate_multi_field_indexes(api: &TestApi) {
    // SQLite must go through a complicated migration procedure which requires dropping and recreating indexes. This test checks that.
    // We run them still against each connector.
    let dm1 = r#"
        model A {
            id Int @id
            field String
            secondField Int

            @@unique([field, secondField])
        }
    "#;

    api.schema_push(dm1).send().assert_green();

    api.assert_schema().assert_table("A", |table| {
        table.assert_index_on_columns(&["field", "secondField"], |idx| idx.assert_is_unique())
    });

    let dm2 = r#"
        model A {
            id    Int    @id
            field String
            secondField Int
            other String

            @@unique([field, secondField])
        }
    "#;

    api.schema_push(dm2).send().assert_green();

    api.assert_schema().assert_table("A", |table| {
        table.assert_index_on_columns(&["field", "secondField"], |idx| idx.assert_is_unique())
    });
}

// This is necessary because of how INTEGER PRIMARY KEY works on SQLite. This has already caused problems.
#[test_case(tags(Sqlite))]
async fn creating_a_model_with_a_non_autoincrement_id_column_is_idempotent(api: TestApi) {
    let dm = r#"
        model Cat {
            id  Int @id
        }
    "#;

    api.schema_push(dm).send().assert_green();
    api.schema_push(dm).send().assert_green().assert_no_steps();
}

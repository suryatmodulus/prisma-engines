use enumflags2::BitFlags;
use migration_engine_tests::the_next_generation::*;
use test_macros::test_case;
use test_setup::connectors::Tags;

#[test_case(tags(Sqlite))]
fn sqlite_must_recreate_indexes(api: TestApi) {
    // SQLite must go through a complicated migration procedure which requires dropping and recreating indexes. This test checks that.
    // We run them still against each connector.
    let dm1 = r#"
        model A {
            id Int @id
            field String @unique
        }
    "#;

    api.schema_push(dm1).send().assert_green();

    api.assert_schema()
        .assert_table("A", |table| {
            table.assert_index_on_columns(&["field"], |idx| idx.assert_is_unique())
        })
        .unwrap();

    let dm2 = r#"
        model A {
            id    Int    @id
            field String @unique
            other String
        }
    "#;

    api.schema_push(dm2).send().assert_green();

    api.assert_schema()
        .assert_table("A", |table| {
            table.assert_index_on_columns(&["field"], |idx| idx.assert_is_unique())
        })
        .unwrap();
}

#[test_case(tags(Sqlite))]
fn sqlite_must_recreate_multi_field_indexes(api: TestApi) {
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

    api.assert_schema()
        .assert_table("A", |table| {
            table.assert_index_on_columns(&["field", "secondField"], |idx| idx.assert_is_unique())
        })
        .unwrap();

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

    api.assert_schema()
        .assert_table("A", |table| {
            table.assert_index_on_columns(&["field", "secondField"], |idx| idx.assert_is_unique())
        })
        .unwrap();
}

// This is necessary because of how INTEGER PRIMARY KEY works on SQLite. This has already caused problems.
#[test_case(tags(Sqlite))]
fn creating_a_model_with_a_non_autoincrement_id_column_is_idempotent(api: TestApi) {
    let dm = r#"
        model Cat {
            id  Int @id
        }
    "#;

    api.schema_push(dm).send().assert_green();
    api.schema_push(dm).send().assert_green().assert_no_steps();
}

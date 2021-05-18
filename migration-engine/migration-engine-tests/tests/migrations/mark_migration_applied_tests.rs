use migration_engine_tests::sql::*;
use pretty_assertions::assert_eq;
use test_macros::test_connector;
use user_facing_errors::{migration_engine::MigrationToMarkAppliedNotFound, UserFacingError};

const BASE_DM: &str = r#"
    model Test {
        id Int @id
    }
"#;

#[test_connector]
async fn mark_migration_applied_on_an_empty_database_works(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;
    let persistence = api.migration_persistence();

    let output = api
        .create_migration("01init", BASE_DM, &migrations_directory)
        .send()
        .await?
        .into_output();

    let migration_name = output.generated_migration_name.unwrap();

    api.assert_schema().await?.assert_tables_count(0)?;

    assert!(
        persistence.list_migrations().await?.is_err(),
        "The migrations table should not be there yet."
    );

    api.mark_migration_applied(&migration_name, &migrations_directory)
        .send()
        .await?;

    let applied_migrations = persistence.list_migrations().await?.unwrap();

    assert_eq!(applied_migrations.len(), 1);
    assert_eq!(&applied_migrations[0].migration_name, &migration_name);
    assert!(&applied_migrations[0].finished_at.is_some());
    assert_eq!(
        &applied_migrations[0].started_at,
        applied_migrations[0].finished_at.as_ref().unwrap()
    );

    api.assert_schema()
        .await?
        .assert_tables_count(1)?
        .assert_has_table("_prisma_migrations")?;

    Ok(())
}

#[test_connector]
async fn mark_migration_applied_on_a_non_empty_database_works(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;
    let persistence = api.migration_persistence();

    // Create and apply a first migration
    let initial_migration_name = {
        let output_initial_migration = api
            .create_migration("01init", BASE_DM, &migrations_directory)
            .send()
            .await?
            .into_output();

        api.apply_migrations(&migrations_directory).send().await?;

        output_initial_migration.generated_migration_name.unwrap()
    };

    // Create a second migration
    let second_migration_name = {
        let dm2 = r#"
            model Test {
                id Int @id
            }

            model Cat {
                id Int @id
                name String
            }
        "#;

        let output_second_migration = api
            .create_migration("02migration", dm2, &migrations_directory)
            .send()
            .await?
            .into_output();

        output_second_migration.generated_migration_name.unwrap()
    };

    // Mark the second migration as applied

    api.mark_migration_applied(&second_migration_name, &migrations_directory)
        .send()
        .await?;

    let applied_migrations = persistence.list_migrations().await?.unwrap();

    assert_eq!(applied_migrations.len(), 2);
    assert_eq!(&applied_migrations[0].migration_name, &initial_migration_name);
    assert!(&applied_migrations[0].finished_at.is_some());
    assert_eq!(&applied_migrations[1].migration_name, &second_migration_name);
    assert!(&applied_migrations[1].finished_at.is_some());
    assert_eq!(
        &applied_migrations[1].started_at,
        applied_migrations[1].finished_at.as_ref().unwrap()
    );

    api.assert_schema()
        .await?
        .assert_tables_count(2)?
        .assert_has_table("_prisma_migrations")?
        .assert_has_table("Test")?;

    Ok(())
}

#[test_connector]
async fn mark_migration_applied_when_the_migration_is_already_applied_errors(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;
    let persistence = api.migration_persistence();

    // Create and apply a first migration
    let initial_migration_name = {
        let output_initial_migration = api
            .create_migration("01init", BASE_DM, &migrations_directory)
            .send()
            .await?
            .into_output();

        output_initial_migration.generated_migration_name.unwrap()
    };

    // Create a second migration
    let second_migration_name = {
        let dm2 = r#"
            model Test {
                id Int @id
            }

            model Cat {
                id Int @id
                name String
            }
        "#;

        let output_second_migration = api
            .create_migration("02migration", dm2, &migrations_directory)
            .send()
            .await?
            .into_output();

        output_second_migration.generated_migration_name.unwrap()
    };

    api.apply_migrations(&migrations_directory).send().await?;

    // Mark the second migration as applied again

    let err = api
        .mark_migration_applied(&second_migration_name, &migrations_directory)
        .send()
        .await
        .unwrap_err();

    assert!(err.to_string().starts_with(&format!(
        "The migration `{}` is already recorded as applied in the database.\n",
        second_migration_name
    )));

    let applied_migrations = persistence.list_migrations().await?.unwrap();

    assert_eq!(applied_migrations.len(), 2);
    assert_eq!(&applied_migrations[0].migration_name, &initial_migration_name);
    assert!(&applied_migrations[0].finished_at.is_some());
    assert_eq!(&applied_migrations[1].migration_name, &second_migration_name);
    assert!(&applied_migrations[1].finished_at.is_some());

    api.assert_schema()
        .await?
        .assert_tables_count(3)?
        .assert_has_table("_prisma_migrations")?
        .assert_has_table("Cat")?
        .assert_has_table("Test")?;

    Ok(())
}

#[test_connector]
async fn mark_migration_applied_when_the_migration_is_failed(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;
    let persistence = api.migration_persistence();

    // Create and apply a first migration
    let initial_migration_name = {
        let output_initial_migration = api
            .create_migration("01init", BASE_DM, &migrations_directory)
            .send()
            .await?
            .into_output();

        output_initial_migration.generated_migration_name.unwrap()
    };

    // Create a second migration
    let second_migration_name = {
        let dm2 = r#"
            model Test {
                id Int @id
            }

            model Cat {
                id Int @id
                name String
            }
        "#;

        let output_second_migration = api
            .create_migration("02migration", dm2, &migrations_directory)
            .send()
            .await?
            .modify_migration(|migration| {
                migration.clear();
                migration.push_str("\nSELECT YOLO;");
            })
            .into_output();

        output_second_migration.generated_migration_name.unwrap()
    };

    api.apply_migrations(&migrations_directory).send().await.ok();

    // Check that the second migration failed.
    {
        let applied_migrations = persistence.list_migrations().await?.unwrap();

        assert_eq!(applied_migrations.len(), 2);
        assert!(
            applied_migrations[1].finished_at.is_none(),
            "The second migration should fail."
        );
    }

    // Mark the second migration as applied again

    api.mark_migration_applied(&second_migration_name, &migrations_directory)
        .send()
        .await?;

    let applied_migrations = persistence.list_migrations().await?.unwrap();

    assert_eq!(applied_migrations.len(), 3);
    assert_eq!(&applied_migrations[0].migration_name, &initial_migration_name);
    assert!(&applied_migrations[0].finished_at.is_some());

    assert_eq!(&applied_migrations[1].migration_name, &second_migration_name);
    assert!(&applied_migrations[1].finished_at.is_none());
    assert!(&applied_migrations[1].rolled_back_at.is_some());

    assert_eq!(&applied_migrations[2].migration_name, &second_migration_name);
    assert!(&applied_migrations[2].finished_at.is_some());
    assert!(&applied_migrations[2].rolled_back_at.is_none());

    api.assert_schema()
        .await?
        .assert_tables_count(2)?
        .assert_has_table("_prisma_migrations")?
        .assert_has_table("Test")?;

    Ok(())
}

#[test_connector]
async fn baselining_should_work(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;
    let persistence = api.migration_persistence();

    let dm1 = r#"
        model test {
            id Int @id
        }
    "#;

    api.schema_push(dm1).send().await?;

    // Create a first local migration that matches the db contents
    let baseline_migration_name = {
        let output_baseline_migration = api
            .create_migration("01baseline", dm1, &migrations_directory)
            .send()
            .await?
            .into_output();

        output_baseline_migration.generated_migration_name.unwrap()
    };

    // Mark the baseline migration as applied
    api.mark_migration_applied(&baseline_migration_name, &migrations_directory)
        .send()
        .await?;

    let applied_migrations = persistence.list_migrations().await?.unwrap();

    assert_eq!(applied_migrations.len(), 1);
    assert_eq!(&applied_migrations[0].migration_name, &baseline_migration_name);
    assert!(&applied_migrations[0].finished_at.is_some());

    api.assert_schema()
        .await?
        .assert_tables_count(2)?
        .assert_has_table("_prisma_migrations")?
        .assert_has_table("test")?;

    Ok(())
}

#[test_connector]
async fn must_return_helpful_error_on_migration_not_found(api: &TestApi) -> TestResult {
    let migrations_directory = api.create_migrations_directory()?;

    let output = api
        .create_migration("01init", BASE_DM, &migrations_directory)
        .send()
        .await?
        .assert_migration_directories_count(1)
        .into_output();

    let migration_name = output.generated_migration_name.unwrap();

    let err = api
        .mark_migration_applied("01init", &migrations_directory)
        .send()
        .await
        .unwrap_err()
        .to_user_facing()
        .unwrap_known();

    assert_eq!(err.error_code, MigrationToMarkAppliedNotFound::ERROR_CODE);

    api.mark_migration_applied(migration_name, &migrations_directory)
        .send()
        .await?;

    Ok(())
}

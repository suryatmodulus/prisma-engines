pub use super::TestResult;
pub use expect_test::expect;
pub use indoc::{formatdoc, indoc};
use migration_connector::CompositeTypeDepth;
use migration_connector::ConnectorResult;
use migration_connector::IntrospectionContext;
use migration_connector::IntrospectionResult;
use migration_connector::Version;
use migration_connector::ViewDefinition;
pub use quaint::prelude::Queryable;
pub use test_macros::test_connector;
pub use test_setup::{BitFlags, Capabilities, Tags};

use crate::{BarrelMigrationExecutor, Result};
use migration_connector::{ConnectorParams, MigrationConnector};
use psl::Configuration;
use psl::PreviewFeature;
use quaint::{prelude::SqlFamily, single::Quaint};
use sql_migration_connector::SqlMigrationConnector;
use std::fmt::Write;
use test_setup::{sqlite_test_url, DatasourceBlock, TestApiArgs};
use tracing::Instrument;

pub struct TestApi {
    pub api: SqlMigrationConnector,
    database: Quaint,
    args: TestApiArgs,
    connection_string: String,
    preview_features: BitFlags<PreviewFeature>,
    namespaces: Vec<String>,
}

impl TestApi {
    pub async fn new(args: TestApiArgs) -> Self {
        let tags = args.tags();
        let connection_string = args.database_url();

        let preview_features = args
            .preview_features()
            .iter()
            .flat_map(|f| PreviewFeature::parse_opt(f))
            .collect();

        let namespaces: Vec<String> = args.namespaces().iter().map(|ns| ns.to_string()).collect();
        let (database, connection_string, api): (Quaint, String, SqlMigrationConnector) =
            if tags.intersects(Tags::Vitess) {
                let mut me = SqlMigrationConnector::new_mysql();

                let params = ConnectorParams {
                    connection_string: connection_string.to_owned(),
                    preview_features,
                    shadow_database_connection_string: None,
                };
                me.set_params(params).unwrap();

                me.reset(true, migration_connector::Namespaces::from_vec(&mut namespaces.clone()))
                    .await
                    .unwrap();

                (
                    Quaint::new(connection_string).await.unwrap(),
                    connection_string.to_owned(),
                    me,
                )
            } else if tags.contains(Tags::Mysql) {
                let (_, cs) = args.create_mysql_database().await;
                let mut me = SqlMigrationConnector::new_mysql();

                let params = ConnectorParams {
                    connection_string: cs.to_owned(),
                    preview_features,
                    shadow_database_connection_string: None,
                };
                me.set_params(params).unwrap();

                (Quaint::new(&cs).await.unwrap(), cs, me)
            } else if tags.contains(Tags::Postgres) {
                let (_, q, cs) = args.create_postgres_database().await;
                if tags.contains(Tags::CockroachDb) {
                    q.raw_cmd(
                        r#"
                    SET default_int_size = 4;
                    "#,
                    )
                    .await
                    .unwrap();
                }

                let mut me = SqlMigrationConnector::new_postgres();

                let params = ConnectorParams {
                    connection_string: cs.to_owned(),
                    preview_features,
                    shadow_database_connection_string: None,
                };
                me.set_params(params).unwrap();

                (q, cs, me)
            } else if tags.contains(Tags::Mssql) {
                let (q, cs) = args.create_mssql_database().await;

                let mut me = SqlMigrationConnector::new_mssql();

                let params = ConnectorParams {
                    connection_string: cs.to_owned(),
                    preview_features,
                    shadow_database_connection_string: None,
                };
                me.set_params(params).unwrap();

                (q, cs, me)
            } else if tags.contains(Tags::Sqlite) {
                let url = sqlite_test_url(args.test_function_name());

                let mut me = SqlMigrationConnector::new_sqlite();

                let params = ConnectorParams {
                    connection_string: url.to_owned(),
                    preview_features,
                    shadow_database_connection_string: None,
                };
                me.set_params(params).unwrap();

                (Quaint::new(&url).await.unwrap(), url, me)
            } else {
                unreachable!()
            };

        TestApi {
            api,
            database,
            args,
            connection_string,
            preview_features,
            namespaces,
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn database(&self) -> &Quaint {
        &self.database
    }

    pub async fn introspect(&mut self) -> Result<String> {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, true).await?;

        Ok(introspection_result.data_model)
    }

    pub async fn introspect_views(&mut self) -> Result<Option<Vec<ViewDefinition>>> {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, true).await?;

        Ok(introspection_result.views)
    }

    pub async fn introspect_dml(&mut self) -> Result<String> {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, false).await?;

        Ok(introspection_result.data_model)
    }

    pub fn is_cockroach(&self) -> bool {
        self.tags().contains(Tags::CockroachDb)
    }

    pub fn is_mysql8(&self) -> bool {
        self.tags().contains(Tags::Mysql8)
    }

    /// Returns true only when testing on vitess.
    pub fn is_vitess(&self) -> bool {
        self.tags().contains(Tags::Vitess)
    }

    pub fn preview_features(&self) -> BitFlags<PreviewFeature> {
        self.preview_features
    }

    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    async fn test_introspect_internal(
        &mut self,
        previous_schema: psl::ValidatedSchema,
        render_config: bool,
    ) -> ConnectorResult<IntrospectionResult> {
        let mut ctx = IntrospectionContext::new(previous_schema, CompositeTypeDepth::Infinite, None);
        ctx.render_config = render_config;

        self.api
            .introspect(&ctx)
            .instrument(tracing::info_span!("introspect"))
            .await
    }

    #[tracing::instrument(skip(self, data_model_string))]
    pub async fn re_introspect(&mut self, data_model_string: &str) -> Result<String> {
        let schema = format!("{}{}", self.pure_config(), data_model_string);
        let schema = parse_datamodel(&schema);
        let introspection_result = self.test_introspect_internal(schema, true).await?;

        Ok(introspection_result.data_model)
    }

    #[tracing::instrument(skip(self, data_model_string))]
    pub async fn re_introspect_dml(&mut self, data_model_string: &str) -> Result<String> {
        let data_model = parse_datamodel(&format!("{}{}", self.pure_config(), data_model_string));
        let introspection_result = self.test_introspect_internal(data_model, false).await?;

        Ok(introspection_result.data_model)
    }

    #[tracing::instrument(skip(self, data_model_string))]
    pub async fn re_introspect_config(&mut self, data_model_string: &str) -> Result<String> {
        let data_model = parse_datamodel(data_model_string);
        let introspection_result = self.test_introspect_internal(data_model, true).await?;

        Ok(introspection_result.data_model)
    }

    pub async fn re_introspect_warnings(&mut self, data_model_string: &str) -> Result<String> {
        let data_model = parse_datamodel(&format!("{}{}", self.pure_config(), data_model_string));
        let introspection_result = self.test_introspect_internal(data_model, false).await?;

        Ok(serde_json::to_string(&introspection_result.warnings)?)
    }

    pub async fn introspect_version(&mut self) -> Result<Version> {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, false).await?;

        Ok(introspection_result.version)
    }

    pub async fn introspection_warnings(&mut self) -> Result<String> {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, false).await?;

        Ok(serde_json::to_string(&introspection_result.warnings)?)
    }

    pub fn sql_family(&self) -> SqlFamily {
        self.database.connection_info().sql_family()
    }

    pub fn schema_name(&self) -> &str {
        self.database.connection_info().schema_name()
    }

    pub fn barrel(&self) -> BarrelMigrationExecutor {
        BarrelMigrationExecutor {
            schema_name: self.schema_name().to_owned(),
            database: self.database.clone(),
            sql_variant: match self.sql_family() {
                SqlFamily::Mysql => barrel::SqlVariant::Mysql,
                SqlFamily::Postgres => barrel::SqlVariant::Pg,
                SqlFamily::Sqlite => barrel::SqlVariant::Sqlite,
                SqlFamily::Mssql => barrel::SqlVariant::Mssql,
            },
            tags: self.tags(),
        }
    }

    pub fn db_name(&self) -> &str {
        if self.tags().intersects(Tags::Vitess) {
            "test"
        } else {
            self.args.test_function_name()
        }
    }

    pub fn tags(&self) -> BitFlags<Tags> {
        self.args.tags()
    }

    pub fn datasource_block_string(&self) -> String {
        let relation_mode = if self.is_vitess() {
            "\nrelationMode = \"prisma\""
        } else {
            ""
        };

        let namespaces: Vec<String> = self.namespaces().iter().map(|ns| format!(r#""{ns}""#)).collect();

        let namespaces = if namespaces.is_empty() {
            "".to_string()
        } else {
            format!("\nschemas = [{}]", namespaces.join(", "))
        };

        let provider = &self.args.provider();
        let datasource_block = format!(
            r#"datasource db {{
                 provider = "{}"
                 url = "{}"{}{}
               }}"#,
            provider, "env(TEST_DATABASE_URL)", namespaces, relation_mode
        );
        datasource_block
    }

    pub fn datasource_block(&self) -> DatasourceBlock<'_> {
        self.args.datasource_block(
            "env(TEST_DATABASE_URL)",
            if self.is_vitess() {
                &[("relationMode", r#""prisma""#)]
            } else {
                &[]
            },
            &[],
        )
    }

    fn pure_config(&self) -> String {
        format!("{}\n{}", &self.datasource_block_string(), &self.generator_block())
    }

    pub fn configuration(&self) -> Configuration {
        psl::parse_configuration(&self.pure_config()).unwrap()
    }

    pub async fn expect_datamodel(&mut self, expectation: &expect_test::Expect) {
        let found = self.introspect().await.unwrap();
        expectation.assert_eq(&found);
    }

    pub async fn expect_view_definition(&mut self, view: &str, expectation: &expect_test::Expect) {
        let views = self.introspect_views().await.unwrap().unwrap_or_default();

        let view = views
            .into_iter()
            .find(|v| v.schema == self.schema_name() && v.name == view)
            .expect("Could not find view with the given name.");

        expectation.assert_eq(&view.definition);
    }

    pub async fn expect_view_definition_in_schema(
        &mut self,
        schema: &str,
        view: &str,
        expectation: &expect_test::Expect,
    ) {
        let views = self.introspect_views().await.unwrap().unwrap_or_default();

        let view = views
            .into_iter()
            .find(|v| v.schema == schema && v.name == view)
            .expect("Could not find view with the given name.");

        expectation.assert_eq(&view.definition);
    }

    pub async fn expect_warnings(&mut self, expectation: &expect_test::Expect) {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, true).await.unwrap();

        expectation.assert_eq(&serde_json::to_string_pretty(&introspection_result.warnings).unwrap());
    }

    pub async fn expect_no_warnings(&mut self) {
        let previous_schema = psl::validate(self.pure_config().into());
        let introspection_result = self.test_introspect_internal(previous_schema, true).await.unwrap();

        dbg!(&introspection_result.warnings);
        assert!(introspection_result.warnings.is_empty())
    }

    pub async fn expect_re_introspected_datamodel(&mut self, schema: &str, expectation: expect_test::Expect) {
        let data_model = parse_datamodel(&format!("{}{}", self.pure_config(), schema));
        let reintrospected = self.test_introspect_internal(data_model, false).await.unwrap();

        expectation.assert_eq(&reintrospected.data_model);
    }

    pub async fn expect_re_introspect_warnings(&mut self, schema: &str, expectation: expect_test::Expect) {
        let data_model = parse_datamodel(&format!("{}{}", self.pure_config(), schema));
        let introspection_result = self.test_introspect_internal(data_model, false).await.unwrap();

        expectation.assert_eq(&serde_json::to_string_pretty(&introspection_result.warnings).unwrap());
    }

    pub fn assert_eq_datamodels(&self, expected_without_header: &str, result_with_header: &str) {
        let expected_with_source = self.dm_with_sources(expected_without_header);
        let expected_with_generator = self.dm_with_generator_and_preview_flags(&expected_with_source);
        let reformatted_expected = psl::reformat(&expected_with_generator, 2).unwrap();

        pretty_assertions::assert_eq!(reformatted_expected, result_with_header);
    }

    fn dm_with_sources(&self, schema: &str) -> String {
        let mut out = String::with_capacity(320 + schema.len());

        write!(out, "{}\n{}", self.datasource_block_string(), schema).unwrap();

        out
    }

    fn dm_with_generator_and_preview_flags(&self, schema: &str) -> String {
        let mut out = String::with_capacity(320 + schema.len());

        write!(out, "{}\n{}", self.generator_block(), schema).unwrap();

        out
    }

    fn generator_block(&self) -> String {
        let preview_features: Vec<String> = self.preview_features().iter().map(|pf| format!(r#""{pf}""#)).collect();

        let preview_feature_string = if preview_features.is_empty() {
            "".to_string()
        } else {
            format!("\npreviewFeatures = [{}]", preview_features.join(", "))
        };

        let generator_block = format!(
            r#"generator client {{
                 provider = "prisma-client-js"{preview_feature_string}
               }}"#
        );
        generator_block
    }

    pub async fn raw_cmd(&self, query: &str) {
        self.database.raw_cmd(query).await.unwrap()
    }
}

#[track_caller]
fn parse_datamodel(dm: &str) -> psl::ValidatedSchema {
    psl::parse_schema(dm).unwrap()
}

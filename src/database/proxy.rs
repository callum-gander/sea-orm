use crate::{ExecResult, ExecResultHolder, QueryResult, QueryResultRow, Statement, error::*};

use sea_query::{ArrayType, Value, ValueType};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

/// Defines the [ProxyDatabaseTrait] to save the functions
#[async_trait::async_trait]
pub trait ProxyDatabaseTrait: Send + Sync + Debug {
    /// Execute a query in the [ProxyDatabase], and return the query results
    async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr>;

    /// Execute a command in the [ProxyDatabase], and report the number of rows affected
    async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr>;

    /// Begin a transaction in the [ProxyDatabase]
    async fn begin(&self) {}

    /// Commit a transaction in the [ProxyDatabase]
    async fn commit(&self) {}

    /// Rollback a transaction in the [ProxyDatabase]
    async fn rollback(&self) {}

    /// Start rollback a transaction in the [ProxyDatabase]
    fn start_rollback(&self) {}

    /// Ping the [ProxyDatabase], it should return an error if the database is not available
    async fn ping(&self) -> Result<(), DbErr> {
        Ok(())
    }
}

/// Defines the results obtained from a [ProxyDatabase]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ProxyExecResult {
    /// The last inserted id on auto-increment
    pub last_insert_id: u64,
    /// The number of rows affected by the database operation
    pub rows_affected: u64,
}

impl ProxyExecResult {
    /// Create a new [ProxyExecResult] from the last inserted id and the number of rows affected
    pub fn new(last_insert_id: u64, rows_affected: u64) -> Self {
        Self {
            last_insert_id,
            rows_affected,
        }
    }
}

impl Default for ExecResultHolder {
    fn default() -> Self {
        Self::Proxy(ProxyExecResult::default())
    }
}

impl From<ProxyExecResult> for ExecResult {
    fn from(result: ProxyExecResult) -> Self {
        Self {
            result: ExecResultHolder::Proxy(result),
        }
    }
}

impl From<ExecResult> for ProxyExecResult {
    fn from(result: ExecResult) -> Self {
        match result.result {
            #[cfg(feature = "sqlx-mysql")]
            ExecResultHolder::SqlxMySql(result) => Self {
                last_insert_id: result.last_insert_id(),
                rows_affected: result.rows_affected(),
            },
            #[cfg(feature = "sqlx-postgres")]
            ExecResultHolder::SqlxPostgres(result) => Self {
                last_insert_id: 0,
                rows_affected: result.rows_affected(),
            },
            #[cfg(feature = "sqlx-sqlite")]
            ExecResultHolder::SqlxSqlite(result) => Self {
                last_insert_id: result.last_insert_rowid() as u64,
                rows_affected: result.rows_affected(),
            },
            #[cfg(feature = "mock")]
            ExecResultHolder::Mock(result) => Self {
                last_insert_id: result.last_insert_id,
                rows_affected: result.rows_affected,
            },
            ExecResultHolder::Proxy(result) => result,
        }
    }
}

/// Defines the structure of a Row for the [ProxyDatabase]
/// which is just a [BTreeMap]<[String], [Value]>
#[derive(Clone, Debug, Default)]
pub struct ProxyRow {
    /// The values of the single row
    pub values: BTreeMap<String, Value>,
    /// Turns a value into the form that a Rust type reads, see [ProxyValueConverter]
    converter: Option<Arc<dyn ProxyValueConverter>>,
}

impl ProxyRow {
    /// Create a new [ProxyRow] from a [BTreeMap]<[String], [Value]>
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Self {
            values,
            converter: None,
        }
    }

    /// Sets the converter that turns the values of this row into the form that a Rust type
    /// reads, see [ProxyValueConverter]
    pub fn with_converter(mut self, converter: Arc<dyn ProxyValueConverter>) -> Self {
        self.converter = Some(converter);
        self
    }
}

/// Turns a value of a [ProxyRow] into the form that a Rust type reads.
///
/// A proxy returns each value in the form that its database stores. SQLite, for example,
/// stores a `bool` as an integer and a date as text. SeaORM reads each Rust type from one
/// variant of [Value]. When the stored variant is another one, [ProxyRow::try_get] passes
/// the value to the converter, with the [ArrayType] of the variant that the Rust type reads.
pub trait ProxyValueConverter: Send + Sync + Debug {
    /// Turns `value`, which is not null, into the variant that `target` names.
    ///
    /// Returns an error when the value has no form in that variant.
    fn convert(&self, value: Value, target: ArrayType) -> Result<Value, DbErr>;
}

impl From<BTreeMap<String, Value>> for ProxyRow {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self::new(values)
    }
}

impl From<ProxyRow> for BTreeMap<String, Value> {
    fn from(row: ProxyRow) -> Self {
        row.values
    }
}

impl From<ProxyRow> for Vec<(String, Value)> {
    fn from(row: ProxyRow) -> Self {
        row.values.into_iter().collect()
    }
}

impl From<ProxyRow> for QueryResult {
    fn from(row: ProxyRow) -> Self {
        QueryResult {
            row: QueryResultRow::Proxy(row),
        }
    }
}

#[cfg(feature = "with-json")]
impl From<ProxyRow> for serde_json::Value {
    fn from(val: ProxyRow) -> serde_json::Value {
        val.values
            .into_iter()
            .map(|(k, v)| (k, sea_query::sea_value_to_json_value(&v)))
            .collect()
    }
}

/// Convert [QueryResult] to [ProxyRow]
pub fn from_query_result_to_proxy_row(result: &QueryResult) -> ProxyRow {
    match &result.row {
        #[cfg(feature = "sqlx-mysql")]
        QueryResultRow::SqlxMySql(row) => crate::from_sqlx_mysql_row_to_proxy_row(row),
        #[cfg(feature = "sqlx-postgres")]
        QueryResultRow::SqlxPostgres(row) => crate::from_sqlx_postgres_row_to_proxy_row(row),
        #[cfg(feature = "sqlx-sqlite")]
        QueryResultRow::SqlxSqlite(row) => crate::from_sqlx_sqlite_row_to_proxy_row(row),
        #[cfg(feature = "mock")]
        QueryResultRow::Mock(row) => ProxyRow::new(row.values.clone()),
        QueryResultRow::Proxy(row) => row.to_owned(),
    }
}

impl ProxyRow {
    /// Get a value from the [ProxyRow]
    ///
    /// A null value, and a column that the row lacks, give [TryGetError::Null], so that an
    /// `Option` field reads as `None`, as with the SQLx drivers. A value of another variant
    /// than the one `T` reads goes through the converter of the row. When the row has no
    /// converter, or the converted value still does not read as `T`, the result is a type
    /// error and not a null.
    pub fn try_get<T, I: crate::ColIdx>(&self, index: I) -> Result<T, TryGetError>
    where
        T: ValueType,
    {
        let value = if let Some(name) = index.as_str() {
            self.values.get(name)
        } else if let Some(position) = index.as_usize() {
            self.values.values().nth(*position)
        } else {
            unreachable!("Missing ColIdx implementation for ProxyRow");
        };
        let Some(value) = value else {
            return Err(TryGetError::Null(format!("{index:?}")));
        };
        if *value == value.as_null() {
            return Err(TryGetError::Null(format!("{index:?}")));
        }
        if let Ok(read) = T::try_from(value.clone()) {
            return Ok(read);
        }
        let converted = match &self.converter {
            Some(converter) => converter.convert(value.clone(), T::array_type())?,
            None => value.clone(),
        };
        T::try_from(converted).map_err(|_| {
            type_err(format!(
                "column {index:?} holds a value of type {:?}, which does not read as {}",
                value.array_type(),
                T::type_name()
            ))
            .into()
        })
    }

    /// An iterator over the keys and values of a proxy row
    pub fn into_column_value_tuples(self) -> impl Iterator<Item = (String, Value)> {
        self.values.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Database, DbBackend, DbErr, ProxyDatabaseTrait, ProxyExecResult, ProxyRow, Statement,
        entity::*, tests_cfg::*,
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct ProxyDb {}

    #[async_trait::async_trait]
    impl ProxyDatabaseTrait for ProxyDb {
        async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr> {
            println!("SQL query: {}", statement.sql);
            Ok(vec![].into())
        }

        async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr> {
            println!("SQL execute: {}", statement.sql);
            Ok(ProxyExecResult {
                last_insert_id: 1,
                rows_affected: 1,
            })
        }
    }

    #[smol_potat::test]
    async fn create_proxy_conn() {
        let _db = Database::connect_proxy(DbBackend::MySql, Arc::new(Box::new(ProxyDb {})))
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn select_rows() {
        let db = Database::connect_proxy(DbBackend::MySql, Arc::new(Box::new(ProxyDb {})))
            .await
            .unwrap();

        let _ = cake::Entity::find().all(&db).await;
    }

    #[smol_potat::test]
    async fn insert_one_row() {
        let db = Database::connect_proxy(DbBackend::MySql, Arc::new(Box::new(ProxyDb {})))
            .await
            .unwrap();

        let item = cake::ActiveModel {
            id: NotSet,
            name: Set("Alice".to_string()),
        };

        cake::Entity::insert(item).exec(&db).await.unwrap();
    }

    /// Reads an integer as a `bool`, as SQLite stores a `bool`.
    #[derive(Debug)]
    struct IntegerAsBool;

    impl crate::ProxyValueConverter for IntegerAsBool {
        fn convert(
            &self,
            value: sea_query::Value,
            target: sea_query::ArrayType,
        ) -> Result<sea_query::Value, DbErr> {
            match (value, target) {
                (sea_query::Value::BigInt(Some(n)), sea_query::ArrayType::Bool) => {
                    Ok(sea_query::Value::Bool(Some(n != 0)))
                }
                (value, _) => Ok(value),
            }
        }
    }

    fn row_with(column: &str, value: sea_query::Value) -> ProxyRow {
        ProxyRow::new([(column.to_owned(), value)].into_iter().collect())
    }

    #[test]
    fn a_value_of_another_type_is_an_error_and_not_a_null() {
        let row = crate::QueryResult::from(row_with("done", sea_query::Value::BigInt(Some(1))));

        assert_eq!(row.try_get::<i64>("", "done"), Ok(1));
        assert!(matches!(
            row.try_get::<bool>("", "done"),
            Err(DbErr::Type(_))
        ));
        assert!(matches!(
            row.try_get::<Option<bool>>("", "done"),
            Err(DbErr::Type(_))
        ));
    }

    #[test]
    fn a_null_and_a_missing_column_read_as_none() {
        let row = crate::QueryResult::from(row_with("done", sea_query::Value::BigInt(None)));

        assert_eq!(row.try_get::<Option<bool>>("", "done"), Ok(None));
        assert_eq!(row.try_get::<Option<bool>>("", "absent"), Ok(None));
        assert!(row.try_get::<bool>("", "done").is_err());
    }

    #[test]
    fn a_converter_turns_a_stored_value_into_the_type_that_is_read() {
        let row = crate::QueryResult::from(
            row_with("done", sea_query::Value::BigInt(Some(1)))
                .with_converter(Arc::new(IntegerAsBool)),
        );

        assert_eq!(row.try_get::<bool>("", "done"), Ok(true));
        assert_eq!(row.try_get::<Option<bool>>("", "done"), Ok(Some(true)));
        assert!(matches!(
            row.try_get::<String>("", "done"),
            Err(DbErr::Type(_))
        ));
    }
}

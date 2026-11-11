use crate::{
    ExecResult, ExecResultHolder, QueryResult, QueryResultRow, Statement, TransactionOptions,
    error::*,
};

use sea_query::{ArrayType, Value, ValueType};
use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

/// Defines the [ProxyDatabaseTrait] to save the functions
#[async_trait::async_trait]
pub trait ProxyDatabaseTrait: Send + Sync + Debug {
    /// Execute a query in the [ProxyDatabase], and return the query results
    async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr>;

    /// Execute a command in the [ProxyDatabase], and report the number of rows affected
    async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr>;

    /// Returns the proxy that runs one new transaction: its `begin`, its statements, and its
    /// `commit` or `rollback`
    ///
    /// A proxy over a database with several connections returns a proxy on a connection of
    /// its own, so that two transactions never share a connection. The default returns
    /// `None`, and the transaction runs on this proxy.
    async fn transaction_proxy(&self) -> Result<Option<Arc<Box<dyn ProxyDatabaseTrait>>>, DbErr> {
        Ok(None)
    }

    /// Begin a transaction in the [ProxyDatabase], or a nested transaction inside the open
    /// one. The options are the ones that the caller passed for the transaction.
    async fn begin(&self, _options: TransactionOptions) -> Result<(), DbErr> {
        Ok(())
    }

    /// Commit a transaction in the [ProxyDatabase]
    async fn commit(&self) -> Result<(), DbErr> {
        Ok(())
    }

    /// Rollback a transaction in the [ProxyDatabase]
    async fn rollback(&self) -> Result<(), DbErr> {
        Ok(())
    }

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
    /// The columns of the row in result order, when the row was made by
    /// [ProxyRow::from_columns]
    columns: Vec<(String, Value)>,
    /// Turns a value into the form that a Rust type reads, see [ProxyValueConverter]
    converter: Option<Arc<dyn ProxyValueConverter>>,
}

impl ProxyRow {
    /// Create a new [ProxyRow] from a [BTreeMap]<[String], [Value]>
    ///
    /// Read by index, such a row follows the order of the names. Use
    /// [ProxyRow::from_columns] to keep the order of the result.
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Self {
            values,
            columns: Vec::new(),
            converter: None,
        }
    }

    /// Create a new [ProxyRow] from its columns in result order
    ///
    /// Read by index, the row follows this order, as the SQLx drivers do: `into_tuple` and
    /// other reads by position need it. Read by name, a name that occurs more than once
    /// gives its last value, as with SQLx.
    pub fn from_columns(columns: Vec<(String, Value)>) -> Self {
        Self {
            values: columns.iter().cloned().collect(),
            columns,
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
        row.into_column_value_tuples().collect()
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
            if self.columns.is_empty() {
                self.values.values().nth(*position)
            } else {
                self.columns.get(*position).map(|(_, value)| value)
            }
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

    /// An iterator over the keys and values of a proxy row, in result order when the row
    /// has one
    pub fn into_column_value_tuples(self) -> impl Iterator<Item = (String, Value)> {
        if self.columns.is_empty() {
            self.values.into_iter().collect::<Vec<_>>().into_iter()
        } else {
            self.columns.into_iter()
        }
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

    /// A proxy that hands each transaction a proxy of its own, and logs where each call ran.
    /// The engine refuses a `BEGIN` or a `COMMIT` when the statement before it says so.
    #[derive(Debug)]
    struct Connections {
        name: String,
        log: Arc<std::sync::Mutex<Vec<String>>>,
        opened: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Connections {
        fn record(&self, call: String) -> Result<(), DbErr> {
            let mut log = self.log.lock().unwrap();
            let refused = log
                .last()
                .is_some_and(|last| last.ends_with(&format!("refuse {call}")));
            log.push(format!("{}: {call}", self.name));
            if refused {
                Err(DbErr::Custom(format!("the engine refused {call}")))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl ProxyDatabaseTrait for Connections {
        async fn query(&self, statement: Statement) -> Result<Vec<ProxyRow>, DbErr> {
            self.record(statement.sql)?;
            Ok(vec![])
        }

        async fn execute(&self, statement: Statement) -> Result<ProxyExecResult, DbErr> {
            self.record(statement.sql)?;
            Ok(ProxyExecResult::default())
        }

        async fn transaction_proxy(
            &self,
        ) -> Result<Option<Arc<Box<dyn ProxyDatabaseTrait>>>, DbErr> {
            let number = self
                .opened
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(Arc::new(Box::new(Connections {
                name: format!("connection {number}"),
                log: self.log.clone(),
                opened: self.opened.clone(),
            }))))
        }

        async fn begin(&self, options: crate::TransactionOptions) -> Result<(), DbErr> {
            match options.sqlite_transaction_mode {
                Some(mode) => self.record(format!("begin {}", mode.sqlite_keyword())),
                None => self.record("begin".to_owned()),
            }
        }

        async fn commit(&self) -> Result<(), DbErr> {
            self.record("commit".to_owned())
        }

        async fn rollback(&self) -> Result<(), DbErr> {
            self.record("rollback".to_owned())
        }
    }

    async fn connections() -> (
        crate::DatabaseConnection,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let proxy = Connections {
            name: "shared".to_owned(),
            log: log.clone(),
            opened: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let db = Database::connect_proxy(DbBackend::Sqlite, Arc::new(Box::new(proxy)))
            .await
            .unwrap();
        (db, log)
    }

    #[smol_potat::test]
    async fn each_transaction_runs_on_a_proxy_of_its_own() {
        use crate::{ConnectionTrait, TransactionTrait};
        let (db, log) = connections().await;

        let first = db.begin().await.unwrap();
        let second = db.begin().await.unwrap();
        first.execute_unprepared("one").await.unwrap();
        second.execute_unprepared("two").await.unwrap();
        first.commit().await.unwrap();
        second.rollback().await.unwrap();
        db.execute_unprepared("three").await.unwrap();

        assert_eq!(
            *log.lock().unwrap(),
            [
                "connection 0: begin",
                "connection 1: begin",
                "connection 0: one",
                "connection 1: two",
                "connection 0: commit",
                "connection 1: rollback",
                "shared: three",
            ]
        );
    }

    #[smol_potat::test]
    async fn a_failed_begin_and_a_failed_commit_reach_the_caller() {
        use crate::{ConnectionTrait, TransactionError, TransactionTrait};
        let (db, _log) = connections().await;

        db.execute_unprepared("refuse begin").await.unwrap();
        let begun = db.begin().await;
        assert_eq!(
            begun.err(),
            Some(DbErr::Custom("the engine refused begin".to_owned()))
        );

        let transaction = db.begin().await.unwrap();
        transaction
            .execute_unprepared("refuse commit")
            .await
            .unwrap();
        assert_eq!(
            transaction.commit().await,
            Err(DbErr::Custom("the engine refused commit".to_owned()))
        );

        let outcome = db
            .transaction::<_, (), DbErr>(|transaction| {
                Box::pin(async move {
                    transaction.execute_unprepared("refuse commit").await?;
                    Ok(())
                })
            })
            .await;
        assert!(matches!(
            outcome,
            Err(TransactionError::Connection(DbErr::Custom(message)))
                if message == "the engine refused commit"
        ));
    }

    #[smol_potat::test]
    async fn the_transaction_options_reach_the_proxy() {
        use crate::{SqliteTransactionMode, TransactionOptions, TransactionTrait};
        let (db, log) = connections().await;

        let transaction = db
            .begin_with_options(TransactionOptions {
                sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .unwrap();
        let nested = transaction.begin().await.unwrap();
        nested.commit().await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            *log.lock().unwrap(),
            [
                "connection 0: begin IMMEDIATE",
                "connection 0: begin",
                "connection 0: commit",
                "connection 0: commit",
            ]
        );
    }

    #[test]
    fn a_row_from_columns_reads_by_index_in_result_order() {
        let row = crate::QueryResult::from(ProxyRow::from_columns(vec![
            ("name".to_owned(), "Ann".into()),
            ("email".to_owned(), "ann@example.org".into()),
            ("id".to_owned(), 1i64.into()),
            ("id".to_owned(), 2i64.into()),
        ]));

        assert_eq!(row.try_get_by_index::<String>(0), Ok("Ann".to_owned()));
        assert_eq!(
            row.try_get_by_index::<String>(1),
            Ok("ann@example.org".to_owned())
        );
        assert_eq!(row.try_get_by_index::<i64>(2), Ok(1));
        assert_eq!(row.try_get_by_index::<i64>(3), Ok(2));
        assert_eq!(row.try_get::<i64>("", "id"), Ok(2));
        assert_eq!(row.column_names(), ["name", "email", "id", "id"]);
    }

    /// A proxy that answers each query with one row of the cake table, the name before the id.
    #[derive(Debug)]
    struct NameThenId;

    #[async_trait::async_trait]
    impl ProxyDatabaseTrait for NameThenId {
        async fn query(&self, _statement: Statement) -> Result<Vec<ProxyRow>, DbErr> {
            Ok(vec![ProxyRow::from_columns(vec![
                ("name".to_owned(), "Chocolate".into()),
                ("id".to_owned(), 1i32.into()),
            ])])
        }

        async fn execute(&self, _statement: Statement) -> Result<ProxyExecResult, DbErr> {
            Ok(ProxyExecResult::default())
        }
    }

    #[smol_potat::test]
    async fn into_tuple_reads_the_columns_in_select_order() {
        use crate::QuerySelect;
        let db = Database::connect_proxy(DbBackend::Sqlite, Arc::new(Box::new(NameThenId)))
            .await
            .unwrap();

        let tuple: Option<(String, i32)> = cake::Entity::find()
            .select_only()
            .column(cake::Column::Name)
            .column(cake::Column::Id)
            .into_tuple()
            .one(&db)
            .await
            .unwrap();

        assert_eq!(tuple, Some(("Chocolate".to_owned(), 1)));
    }

    #[smol_potat::test]
    async fn a_stream_yields_the_rows_of_the_proxy() {
        use crate::TransactionTrait;
        use futures_util::TryStreamExt;
        let db = Database::connect_proxy(DbBackend::Sqlite, Arc::new(Box::new(NameThenId)))
            .await
            .unwrap();
        let chocolate = cake::Model {
            id: 1,
            name: "Chocolate".to_owned(),
        };

        let streamed: Vec<cake::Model> = cake::Entity::find()
            .stream(&db)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(streamed, [chocolate.clone()]);

        let transaction = db.begin().await.unwrap();
        let streamed: Vec<cake::Model> = cake::Entity::find()
            .stream(&transaction)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(streamed, [chocolate]);
    }
}

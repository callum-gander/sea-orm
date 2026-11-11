use crate::{
    DatabaseConnection, DatabaseConnectionType, DbBackend, ExecResult, ProxyDatabaseTrait,
    QueryResult, Statement, TransactionOptions, debug_print, error::*,
};
use std::{fmt::Debug, sync::Arc};
use tracing::instrument;

/// Defines a database driver for the [ProxyDatabase]
#[derive(Debug)]
pub struct ProxyDatabaseConnector;

/// Defines a connection for the [ProxyDatabase]
#[derive(Debug)]
pub struct ProxyDatabaseConnection {
    db_backend: DbBackend,
    proxy: Arc<Box<dyn ProxyDatabaseTrait>>,
}

impl ProxyDatabaseConnector {
    /// Check if the database URI given and the [DatabaseBackend](crate::DatabaseBackend) selected are the same
    #[allow(unused_variables)]
    pub fn accepts(string: &str) -> bool {
        // As this is a proxy database, it accepts any URI
        true
    }

    /// Connect to the [ProxyDatabase]
    #[allow(unused_variables)]
    #[instrument(level = "trace")]
    pub fn connect(
        db_type: DbBackend,
        func: Arc<Box<dyn ProxyDatabaseTrait>>,
    ) -> Result<DatabaseConnection, DbErr> {
        Ok(
            DatabaseConnectionType::ProxyDatabaseConnection(Arc::new(
                ProxyDatabaseConnection::new(db_type, func),
            ))
            .into(),
        )
    }
}

impl ProxyDatabaseConnection {
    /// Create a connection to the [ProxyDatabase]
    pub fn new(db_backend: DbBackend, funcs: Arc<Box<dyn ProxyDatabaseTrait>>) -> Self {
        Self {
            db_backend,
            proxy: funcs.to_owned(),
        }
    }

    /// Get the [DatabaseBackend](crate::DatabaseBackend) being used by the [ProxyDatabase]
    pub fn get_database_backend(&self) -> DbBackend {
        self.db_backend
    }

    /// Execute the SQL statement in the [ProxyDatabase]
    #[instrument(level = "trace", skip(statement))]
    pub async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        debug_print!("{}", statement);
        Ok(self.proxy.execute(statement).await?.into())
    }

    /// Return one [QueryResult] if the query was successful
    #[instrument(level = "trace", skip(statement))]
    pub async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        debug_print!("{}", statement);
        let result = self.proxy.query(statement).await?;

        if let Some(first) = result.first() {
            return Ok(Some(QueryResult {
                row: crate::QueryResultRow::Proxy(first.to_owned()),
            }));
        } else {
            return Ok(None);
        }
    }

    /// Return all [QueryResult]s if the query was successful
    #[instrument(level = "trace", skip(statement))]
    pub async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        debug_print!("{}", statement);
        let result = self.proxy.query(statement).await?;

        Ok(result
            .into_iter()
            .map(|row| QueryResult {
                row: crate::QueryResultRow::Proxy(row),
            })
            .collect())
    }

    /// Create a statement block  of SQL statements that execute together.
    #[instrument(level = "trace")]
    pub async fn begin(&self, options: TransactionOptions) -> Result<(), DbErr> {
        self.proxy.begin(options).await
    }

    /// Commit a transaction atomically to the database
    #[instrument(level = "trace")]
    pub async fn commit(&self) -> Result<(), DbErr> {
        self.proxy.commit().await
    }

    /// Roll back a faulty transaction
    #[instrument(level = "trace")]
    pub async fn rollback(&self) -> Result<(), DbErr> {
        self.proxy.rollback().await
    }

    /// Start rollback a transaction
    #[instrument(level = "trace")]
    pub fn start_rollback(&self) {
        self.proxy.start_rollback()
    }

    /// Checks if a connection to the database is still valid.
    pub async fn ping(&self) -> Result<(), DbErr> {
        self.proxy.ping().await
    }
}

#[cfg(feature = "stream")]
impl
    From<(
        Arc<crate::ProxyDatabaseConnection>,
        Statement,
        Option<crate::metric::Callback>,
    )> for crate::QueryStream
{
    fn from(
        (conn, stmt, metric_callback): (
            Arc<crate::ProxyDatabaseConnection>,
            Statement,
            Option<crate::metric::Callback>,
        ),
    ) -> Self {
        crate::QueryStream::build(stmt, crate::InnerConnection::Proxy(conn), metric_callback)
    }
}

/// The rows of a query on a proxy, as a stream
///
/// A proxy returns all rows of a query at once, so the stream yields the rows of one call, as
/// the rusqlite driver does.
#[cfg(feature = "stream")]
pub(crate) fn proxy_rows_stream<'a>(
    conn: &'a ProxyDatabaseConnection,
    statement: &Statement,
) -> impl futures_util::Stream<Item = Result<QueryResult, DbErr>> + Send + 'a {
    use futures_util::StreamExt;

    futures_util::stream::once(conn.query_all(statement.clone())).flat_map(|rows| {
        futures_util::stream::iter(match rows {
            Ok(rows) => itertools::Either::Left(rows.into_iter().map(Ok)),
            Err(error) => itertools::Either::Right(std::iter::once(Err(error))),
        })
    })
}

impl crate::DatabaseTransaction {
    pub(crate) async fn new_proxy(
        inner: Arc<crate::ProxyDatabaseConnection>,
        metric_callback: Option<crate::metric::Callback>,
        options: TransactionOptions,
    ) -> Result<crate::DatabaseTransaction, DbErr> {
        use futures_util::lock::Mutex;
        let backend = inner.get_database_backend();
        // The proxy can run the transaction on a connection of its own.
        let inner = match inner.proxy.transaction_proxy().await? {
            Some(proxy) => Arc::new(crate::ProxyDatabaseConnection::new(backend, proxy)),
            None => inner,
        };
        Self::begin(
            Arc::new(Mutex::new(crate::InnerConnection::Proxy(inner))),
            backend,
            metric_callback,
            true,
            options.isolation_level,
            options.access_mode,
            options.sqlite_transaction_mode,
        )
        .await
    }
}

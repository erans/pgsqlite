use std::sync::Arc;

use futures::SinkExt;
use tokio_util::codec::Framed;

use crate::protocol::{
    check_global_rate_limit, BackendMessage, ErrorResponse, FrontendMessage, PostgresCodec,
};
use crate::query::ExtendedQueryHandler;
use crate::session::{DbHandler, SessionState};

#[derive(Clone, Copy, Debug)]
pub struct ExtendedMessageOptions {
    pub enforce_parse_rate_limit: bool,
    pub send_ready_after_error: bool,
    pub unsupported_as_error: bool,
}

impl ExtendedMessageOptions {
    pub const fn test_defaults() -> Self {
        Self {
            enforce_parse_rate_limit: false,
            send_ready_after_error: false,
            unsupported_as_error: true,
        }
    }

    pub const fn server_defaults() -> Self {
        Self {
            enforce_parse_rate_limit: true,
            send_ready_after_error: true,
            unsupported_as_error: false,
        }
    }
}

async fn send_ready_for_query<T>(
    framed: &mut Framed<T, PostgresCodec>,
    session: &Arc<SessionState>,
) -> anyhow::Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    framed
        .send(BackendMessage::ReadyForQuery {
            status: *session.transaction_status.read().await,
        })
        .await?;
    Ok(())
}

async fn send_extended_error<T>(
    framed: &mut Framed<T, PostgresCodec>,
    session: &Arc<SessionState>,
    code: &str,
    message: String,
    send_ready_after_error: bool,
) -> anyhow::Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let err = ErrorResponse::new("ERROR".to_string(), code.to_string(), message);
    framed
        .send(BackendMessage::ErrorResponse(Box::new(err)))
        .await?;

    if send_ready_after_error {
        send_ready_for_query(framed, session).await?;
    }

    Ok(())
}

/// Handles non-simple-query frontend messages that are shared between test and server loops.
/// Returns `true` when the message was handled in this helper.
pub async fn handle_extended_or_aux_message<T>(
    framed: &mut Framed<T, PostgresCodec>,
    db_handler: &Arc<DbHandler>,
    session: &Arc<SessionState>,
    message: FrontendMessage,
    options: ExtendedMessageOptions,
) -> anyhow::Result<bool>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    match message {
        FrontendMessage::Parse {
            name,
            query,
            param_types,
        } => {
            if options.enforce_parse_rate_limit && check_global_rate_limit(None).is_err() {
                send_extended_error(
                    framed,
                    session,
                    "53300",
                    "Rate limit exceeded".to_string(),
                    false,
                )
                .await?;
                return Ok(true);
            }

            if let Err(e) = ExtendedQueryHandler::handle_parse(
                framed,
                db_handler,
                session,
                name,
                query,
                param_types,
            )
            .await
            {
                send_extended_error(
                    framed,
                    session,
                    "42000",
                    format!("Parse failed: {e}"),
                    options.send_ready_after_error,
                )
                .await?;
            }
            Ok(true)
        }
        FrontendMessage::Bind {
            portal,
            statement,
            formats,
            values,
            result_formats,
        } => {
            if let Err(e) = ExtendedQueryHandler::handle_bind(
                framed,
                session,
                portal,
                statement,
                formats,
                values,
                result_formats,
            )
            .await
            {
                send_extended_error(
                    framed,
                    session,
                    "42000",
                    format!("Bind failed: {e}"),
                    options.send_ready_after_error,
                )
                .await?;
            }
            Ok(true)
        }
        FrontendMessage::Execute { portal, max_rows } => {
            if let Err(e) =
                ExtendedQueryHandler::handle_execute(framed, db_handler, session, portal, max_rows)
                    .await
            {
                send_extended_error(
                    framed,
                    session,
                    "42000",
                    format!("Execute failed: {e}"),
                    options.send_ready_after_error,
                )
                .await?;
            }
            Ok(true)
        }
        FrontendMessage::Describe { typ, name } => {
            if let Err(e) = ExtendedQueryHandler::handle_describe(framed, session, typ, name).await {
                send_extended_error(
                    framed,
                    session,
                    "42000",
                    format!("Describe failed: {e}"),
                    options.send_ready_after_error,
                )
                .await?;
            }
            Ok(true)
        }
        FrontendMessage::Close { typ, name } => {
            if let Err(e) = ExtendedQueryHandler::handle_close(framed, session, typ, name).await {
                send_extended_error(
                    framed,
                    session,
                    "42000",
                    format!("Close failed: {e}"),
                    options.send_ready_after_error,
                )
                .await?;
            }
            Ok(true)
        }
        FrontendMessage::Sync => {
            send_ready_for_query(framed, session).await?;
            framed.flush().await?;
            Ok(true)
        }
        FrontendMessage::Flush => {
            framed.flush().await?;
            Ok(true)
        }
        FrontendMessage::Query(_) | FrontendMessage::Terminate => Ok(false),
        other => {
            if options.unsupported_as_error {
                send_extended_error(
                    framed,
                    session,
                    "0A000",
                    format!("Feature not supported: {other:?}"),
                    true,
                )
                .await?;
            }
            Ok(true)
        }
    }
}
